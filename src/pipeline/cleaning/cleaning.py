from __future__ import annotations

import pandas as pd

from pipeline.common.config import PipelineConfig
from pipeline.common.contracts import PipelineContext, PipelineExecutionError


def run_cleaning(context: PipelineContext, config: PipelineConfig) -> None:
    if context.raw_df is None:
        raise PipelineExecutionError(stage="cleaning", message="Cleaning requires an ingested dataframe.")

    df = context.raw_df.copy(deep=True)

    for column in list(df.columns):
        series = df[column]
        if _is_string_like(series):
            stripped = series.map(lambda value: value.strip() if isinstance(value, str) else value)
            changed = int(((series != stripped) & series.notna()).sum())
            if changed > 0:
                df[column] = stripped
                context.log_transform(action="stripped_whitespace", column=column, rows_affected=changed)

            if config.cleaning.case_strategy == "lower":
                lowered = df[column].map(lambda value: value.lower() if isinstance(value, str) else value)
                changed = int(((df[column] != lowered) & df[column].notna()).sum())
                if changed > 0:
                    df[column] = lowered
                    context.log_transform(action="normalized_case", column=column, rows_affected=changed)

        semantic_type = context.schema.get(column, {}).get("semantic_type")
        if semantic_type == "date":
            converted = pd.to_datetime(df[column], errors="coerce")
            changed = int((converted.notna() & df[column].notna()).sum())
            if changed > 0:
                df[column] = converted

        if config.cleaning.auto_convert_numeric_strings and semantic_type != "date":
            converted = pd.to_numeric(df[column], errors="coerce")
            original_non_null = df[column].notna().sum()
            ratio = (converted.notna().sum() / original_non_null) if original_non_null else 0.0
            if ratio >= config.cleaning.numeric_parse_threshold and not pd.api.types.is_numeric_dtype(df[column]):
                df[column] = converted
                context.log_transform(
                    action="converted_numeric",
                    column=column,
                    rows_affected=int(converted.notna().sum()),
                )

    if config.cleaning.always_drop_fully_missing_columns:
        for column in list(df.columns):
            if df[column].isna().all():
                df = df.drop(columns=[column])
                context.log_transform(action="dropped_fully_missing_column", column=column, rows_affected=len(context.raw_df))
                context.add_warning(f"Column {column} dropped: 100% missing.")

    if config.cleaning.always_drop_zero_variance_columns:
        for column in list(df.columns):
            non_null = df[column].dropna()
            if not non_null.empty and non_null.nunique() <= 1:
                df = df.drop(columns=[column])
                context.log_transform(action="dropped_zero_variance_column", column=column, rows_affected=len(non_null))
                context.add_warning(f"Column {column} dropped: zero variance.")

    for column in df.select_dtypes(include=["number"]).columns:
        missing_count = int(df[column].isna().sum())
        if missing_count > 0 and config.cleaning.numeric_missing_strategy == "median":
            fill_value = df[column].median()
            df[column] = df[column].fillna(fill_value)
            context.log_transform(action="imputed_median", column=column, rows_affected=missing_count)

    for column in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[column]):
            missing_count = int(df[column].isna().sum())
            if missing_count > 0 and config.cleaning.date_missing_strategy == "forward_fill":
                df[column] = df[column].ffill()
                context.log_transform(action="ffill_date", column=column, rows_affected=missing_count)

    categorical_columns = [
        column
        for column in df.columns
        if column not in df.select_dtypes(include=["number"]).columns
        and not pd.api.types.is_datetime64_any_dtype(df[column])
    ]
    for column in categorical_columns:
        missing_count = int(df[column].isna().sum())
        if missing_count == 0:
            continue
        if config.cleaning.categorical_missing_strategy == "mode":
            mode = df[column].mode(dropna=True)
            fill_value = mode.iloc[0] if not mode.empty else config.cleaning.categorical_fill_value
        else:
            fill_value = config.cleaning.categorical_fill_value
        df[column] = df[column].fillna(fill_value)
        context.log_transform(action="imputed_mode", column=column, rows_affected=missing_count)

    if config.cleaning.duplicate_strategy == "drop_duplicates":
        duplicates_count = int(df.duplicated().sum())
        if duplicates_count > 0:
            df = df.drop_duplicates(keep="first").reset_index(drop=True)
            context.log_transform(action="dropped_duplicates", rows_affected=duplicates_count)

    context.clean_df = df


def _is_string_like(series: pd.Series) -> bool:
    return str(series.dtype) in {"object", "string"} or pd.api.types.is_string_dtype(series)
