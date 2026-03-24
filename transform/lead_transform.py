import numpy as np
import pandas as pd


def parse_excel_mixed_datetime(series: pd.Series) -> pd.Series:
    """
    处理Excel混合日期格式：
    1) Excel序列号
    2) 字符串日期
    3) 原生datetime
    """
    s = series.copy()
    out = pd.to_datetime(s, errors="coerce")

    # 仅在值看起来像Excel日期序列号（天）时才按序列号解析，
    # 避免把原生Timestamp转成纳秒整数后误覆盖为NaT。
    num = pd.to_numeric(s, errors="coerce")
    excel_serial_mask = (num >= 1) & (num <= 80000)
    if excel_serial_mask.any():
        dt_from_num = pd.to_datetime(
            num.where(excel_serial_mask),
            unit="D",
            origin="1899-12-30",
            errors="coerce",
        )
        out = out.where(~excel_serial_mask, dt_from_num)

    return out


def combine_dt(df: pd.DataFrame, time_col: str, date_col: str) -> pd.Series:
    """优先使用时间列，缺失时回填日期列。"""
    return df[time_col].combine_first(df[date_col])


def to_binary_from_dt(series: pd.Series) -> pd.Series:
    """日期不为空即视为1。"""
    return series.notna().astype(int)


def ensure_str(df: pd.DataFrame, cols):
    """类别字段统一清洗。"""
    for c in cols:
        df[c] = (
            df[c]
            .fillna("未知")
            .astype(str)
            .str.strip()
            .replace("", "未知")
        )
    return df


def normalize_level(value, level_score_map: dict) -> str:
    """线索等级标准化，仅保留配置中的等级。"""
    if pd.isna(value):
        return "NA"
    s = str(value).strip().upper()
    if s == "" or s in {"NAN", "NONE", "NULL"}:
        return "NA"
    return s if s in level_score_map else "NA"


def classify_status_group(row: pd.Series) -> str:
    """线索状态分组规则。"""
    status_text = str(row.get("线索状态", ""))

    if row["是否成交"] == 1:
        return "已成交"
    if row["是否下订"] == 1:
        return "已下订"
    if row["是否试驾"] == 1:
        return "已试驾"
    if row["是否到店"] == 1:
        return "已到店"
    if pd.notna(row["_战败DT"]) or any(k in status_text for k in ["战败", "无效", "失效"]):
        return "无效战败"
    if pd.notna(row["_下发DT"]) or ("邀约" in status_text):
        return "已邀约"
    return "其他"


def prepare_lead_dataframe(
    input_file: str,
    sheet_name: str,
    expected_columns: list,
    time_date_columns: list,
    level_score_map: dict,
    level_desc_map: dict,
):
    """
    统一完成：
    1) 加载与清洗
    2) 主键去重（手机号优先，线索ID+创建时间兜底）
    3) 衍生指标
    """
    file_lower = str(input_file).lower()
    if file_lower.endswith(".csv"):
        last_err = None
        for enc in ["utf-8-sig", "gb18030", "gbk"]:
            try:
                df = pd.read_csv(input_file, encoding=enc)
                break
            except UnicodeDecodeError as e:
                last_err = e
        else:
            raise last_err
    else:
        if sheet_name and str(sheet_name).strip() != "":
            try:
                df = pd.read_excel(input_file, sheet_name=sheet_name)
            except ValueError:
                # 指定sheet不存在时回退到第一张sheet，避免文件日期变更导致脚本失效。
                df = pd.read_excel(input_file, sheet_name=0)
        else:
            df = pd.read_excel(input_file, sheet_name=0)

    missing_cols = [c for c in expected_columns if c not in df.columns]
    if missing_cols:
        raise ValueError(f"缺少以下必要列: {missing_cols}")

    # 保留并按指定字段顺序排列
    df = df[expected_columns].copy()
    raw_rows = len(df)

    # 去除全空行
    df = df.dropna(how="all").copy()
    non_empty_rows = len(df)

    # 转换跟进次数为数值
    df["跟进次数"] = pd.to_numeric(df["跟进次数"], errors="coerce").fillna(0)

    # 时间/日期列统一转datetime（含Excel序列号处理）
    for col in time_date_columns:
        df[col] = parse_excel_mixed_datetime(df[col])

    # 构建统一事件时间（时间优先，日期兜底）
    df["_创建DT"] = combine_dt(df, "创建时间", "创建日期")
    df["_下发DT"] = combine_dt(df, "下发时间", "下发日期")
    df["_到店DT"] = combine_dt(df, "到店时间", "到店日期")
    df["_试驾DT"] = combine_dt(df, "试驾时间", "试驾日期")
    df["_下订DT"] = combine_dt(df, "下订时间", "下订日期")
    df["_成交DT"] = combine_dt(df, "成交时间", "成交日期")
    df["_首次跟进DT"] = combine_dt(df, "首次跟进时间", "首次跟进日期")
    df["_战败DT"] = combine_dt(df, "战败时间", "战败日期")

    # 主键去重口径（新）
    # 业务主键优先：手机号
    # 辅助主键：线索ID + 创建时间
    df["_手机号_norm"] = (
        df["手机号"]
        .fillna("")
        .astype(str)
        .str.replace(r"\D+", "", regex=True)
        .str.strip()
    )
    df["_线索ID_norm"] = df["线索ID"].fillna("").astype(str).str.strip()
    df["_创建DT_key"] = df["_创建DT"].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")

    df["_主键_手机号"] = "PHONE:" + df["_手机号_norm"]
    df["_主键_线索时间"] = "LEAD_CREATE:" + df["_线索ID_norm"] + "|" + df["_创建DT_key"]
    df["_业务主键"] = np.where(
        df["_手机号_norm"].ne(""),
        df["_主键_手机号"],
        np.where(
            df["_线索ID_norm"].ne("") & df["_创建DT_key"].ne(""),
            df["_主键_线索时间"],
            "ROW:" + df.index.astype(str),
        ),
    )

    df["_排序时间"] = df["_创建DT"].fillna(pd.Timestamp.max)
    df["_原始行号"] = np.arange(len(df))
    df = (
        df.sort_values(["_业务主键", "_排序时间", "_原始行号"], kind="stable")
        .drop_duplicates(subset=["_业务主键"], keep="first")
        .copy()
    )
    dedup_rows = len(df)

    # 时长类指标（单位：天）
    df["到店时长_天"] = (df["_到店DT"] - df["_下发DT"]).dt.total_seconds() / 86400
    df["试驾转化时长_天"] = (df["_试驾DT"] - df["_到店DT"]).dt.total_seconds() / 86400
    df["成交周期_天"] = (df["_成交DT"] - df["_创建DT"]).dt.total_seconds() / 86400

    # 负时长置空，避免异常值污染
    for c in ["到店时长_天", "试驾转化时长_天", "成交周期_天"]:
        df.loc[df[c] < 0, c] = np.nan

    # 漏斗状态0/1指标
    df["是否到店"] = to_binary_from_dt(df["_到店DT"])
    df["是否试驾"] = to_binary_from_dt(df["_试驾DT"])
    df["是否下订"] = to_binary_from_dt(df["_下订DT"])
    df["是否成交"] = to_binary_from_dt(df["_成交DT"])

    # 车型匹配：成交车型 == 意向车型（且成交车型非空）
    deal_model = df["成交车型"].fillna("").astype(str).str.strip()
    intent_model = df["意向车型"].fillna("").astype(str).str.strip()
    df["车型匹配"] = ((deal_model != "") & (deal_model == intent_model)).astype(int)

    # 线索状态分组
    df["线索状态分组"] = df.apply(classify_status_group, axis=1)

    # 直播间ID是否有值（用于分析切片）
    df["直播间ID_有值"] = (
        df["直播间ID"]
        .apply(lambda x: 0 if pd.isna(x) or str(x).strip() == "" else 1)
        .astype(int)
    )

    # 线索等级标准化与排序分值（O > H > A > B > C > D > F > NA）
    df["线索等级标准"] = df["线索等级"].apply(lambda x: normalize_level(x, level_score_map))
    df["线索等级得分"] = df["线索等级标准"].map(level_score_map).fillna(1).astype(int)
    df["线索等级说明"] = df["线索等级标准"].map(level_desc_map).fillna(level_desc_map["NA"])

    # 总部新媒体专项衍生字段
    # 1) 时间维度：响应延迟（首次跟进 - 创建）
    df["响应延迟_分钟"] = (df["_首次跟进DT"] - df["_创建DT"]).dt.total_seconds() / 60
    df["响应延迟_小时"] = df["响应延迟_分钟"] / 60

    # 2) 时间维度：创建日期 vs 成交日期（按自然日）
    df["转化周期_天"] = (
        df["_成交DT"].dt.normalize() - df["_创建DT"].dt.normalize()
    ).dt.days.astype(float)

    # 异常负值置空
    for c in ["响应延迟_分钟", "响应延迟_小时", "转化周期_天"]:
        df.loc[df[c] < 0, c] = np.nan

    # 3) 渠道组合：渠道1+渠道2+渠道3
    channel_combo_df = ensure_str(df.copy(), ["渠道1", "渠道2", "渠道3"])
    df["渠道组合"] = (
        channel_combo_df["渠道1"] + " | " + channel_combo_df["渠道2"] + " | " + channel_combo_df["渠道3"]
    )

    # 4) 客户意向分析：首次意向车型 vs 实际成交车型
    first_intent_model = df["首次意向车型"].fillna("").astype(str).str.strip()
    deal_model_clean = df["成交车型"].fillna("").astype(str).str.strip()
    df["是否变更意向"] = (
        (df["是否成交"] == 1)
        & (first_intent_model != "")
        & (deal_model_clean != "")
        & (first_intent_model != deal_model_clean)
    ).astype(int)

    stats = {
        "raw_rows": raw_rows,
        "non_empty_rows": non_empty_rows,
        "dedup_rows": dedup_rows,
    }
    return df, stats

