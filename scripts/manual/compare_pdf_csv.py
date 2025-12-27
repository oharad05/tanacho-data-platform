#!/usr/bin/env python3
"""
PDF CSVとスプレッドシートCSVの比較スクリプト
"""

import pandas as pd
import re
import os
from datetime import datetime
from pathlib import Path

# プロジェクトルート
PROJECT_ROOT = Path(__file__).parent.parent.parent

# 設定
BRANCHES = {
    "東京支店": {
        "pdf_prefix": "BI テスト - pdf_東京_",
        "csv_branch_header": "東京支店",
    },
    "長崎支店": {
        "pdf_prefix": "BI テスト - pdf_長崎_",
        "csv_branch_header": "長崎支店",
    },
    "福岡支店": {
        "pdf_prefix": "BI テスト - pdf_福岡_",
        "csv_branch_header": "福岡支店",
    },
}

MONTHS = ["202509", "202510", "202511"]


def normalize_secondary_category(cat: str) -> str:
    """secondary_categoryを正規化（千円、%を削除、括弧を統一）"""
    if pd.isna(cat):
        return ""
    cat = str(cat)
    # (千円)、(%)を削除
    cat = re.sub(r"[\(（](千円|%)[\)）]", "", cat)
    # 全角括弧を半角に統一
    cat = cat.replace("（", "(").replace("）", ")")
    return cat.strip()


def parse_number(val) -> float:
    """数値をパース（カンマ区切り、%記号対応）"""
    if pd.isna(val) or val == "" or val == "-":
        return None
    val = str(val)
    # %を削除
    val = val.replace("%", "")
    # カンマを削除
    val = val.replace(",", "")
    # 括弧でマイナスを表現している場合
    if val.startswith("(") and val.endswith(")"):
        val = "-" + val[1:-1]
    try:
        return float(val)
    except ValueError:
        return None


def normalize_main_category(cat: str) -> str:
    """main_categoryを正規化"""
    if pd.isna(cat):
        return ""
    cat = str(cat)
    # 売総利益率 → 売上総利益率
    if cat == "売総利益率":
        cat = "売上総利益率"
    return cat.strip()


def normalize_department_name(name: str) -> str:
    """部門名を正規化（括弧を統一）"""
    if pd.isna(name):
        return ""
    name = str(name)
    # 全角括弧を半角に統一
    name = name.replace("（", "(").replace("）", ")")
    return name.strip()


def load_pdf_data(branch: str, month: str) -> pd.DataFrame:
    """PDFデータを読み込み"""
    pdf_prefix = BRANCHES[branch]["pdf_prefix"]
    pdf_path = PROJECT_ROOT / "sql" / "tmp_file" / "csv_from_pdf" / f"{pdf_prefix}{month}.csv"

    if not pdf_path.exists():
        print(f"PDF file not found: {pdf_path}")
        return pd.DataFrame()

    df = pd.read_csv(pdf_path, encoding="utf-8")

    # 区分（大）が空の場合は前の値を引き継ぐ（PDFの構造上）
    if "区分（大）" in df.columns:
        df["区分（大）"] = df["区分（大）"].fillna(method="ffill")

    return df


def load_csv_data_for_branch(month: str, branch: str) -> pd.DataFrame:
    """スプレッドシートCSVデータを支店ごとに読み込み"""
    csv_path = PROJECT_ROOT / "sql" / "tmp_file" / f"SSでの可視化 - PL(月単位)_{month}.csv"

    if not csv_path.exists():
        print(f"CSV file not found: {csv_path}")
        return pd.DataFrame()

    # ヘッダー行を2行読み込む
    branch_header_row = pd.read_csv(csv_path, encoding="utf-8", nrows=1, header=None)
    dept_header_row = pd.read_csv(csv_path, encoding="utf-8", skiprows=1, nrows=1, header=None)
    data_df = pd.read_csv(csv_path, encoding="utf-8", skiprows=2, header=None)

    # 支店のカラム範囲を特定
    csv_branch_header = BRANCHES[branch]["csv_branch_header"]
    branch_row = branch_header_row.iloc[0]

    # 支店の開始・終了インデックスを特定
    start_idx = None
    end_idx = None

    for i, val in enumerate(branch_row):
        if pd.notna(val) and str(val).strip() == csv_branch_header:
            start_idx = i
        elif start_idx is not None and pd.notna(val) and str(val).strip() in ["長崎支店", "福岡支店", ""]:
            if str(val).strip() != csv_branch_header and str(val).strip() != "":
                end_idx = i
                break

    # 終了インデックスが見つからない場合は最後まで
    if start_idx is not None and end_idx is None:
        # 次の支店を探す
        next_branches = [b for b in ["東京支店", "長崎支店", "福岡支店"] if b != csv_branch_header]
        for i in range(start_idx + 1, len(branch_row)):
            val = branch_row.iloc[i]
            if pd.notna(val) and str(val).strip() in next_branches:
                end_idx = i
                break
        if end_idx is None:
            end_idx = len(branch_row)

    if start_idx is None:
        print(f"Branch {branch} not found in CSV headers")
        return pd.DataFrame()

    # main_category と secondary_category のカラムは常に0,1
    selected_cols = [0, 1] + list(range(start_idx, end_idx))

    # 該当カラムのみ抽出
    dept_headers = dept_header_row.iloc[0, start_idx:end_idx].tolist()
    # main_category, secondary_category を追加
    all_headers = ["main_category", "secondary_category"] + [normalize_department_name(str(h)) for h in dept_headers]

    result_df = data_df.iloc[:, selected_cols].copy()
    result_df.columns = all_headers

    return result_df


def compare_branch_month(branch: str, month: str) -> list:
    """支店・月ごとにPDFとCSVを比較"""
    results = []

    # データ読み込み
    pdf_df = load_pdf_data(branch, month)
    csv_df = load_csv_data_for_branch(month, branch)

    if pdf_df.empty or csv_df.empty:
        return results

    # PDF行をループ
    for _, pdf_row in pdf_df.iterrows():
        pdf_main_cat = normalize_main_category(pdf_row.get("区分（大）", ""))
        pdf_sec_cat = normalize_secondary_category(pdf_row.get("区分（小）", ""))

        # 前年比%、目標比%は除外
        if "前年比" in pdf_sec_cat or "目標比" in pdf_sec_cat:
            continue

        # 経常利益の特殊なsecondary_category対応
        csv_sec_cat_match = pdf_sec_cat
        if pdf_sec_cat in ["当月経常利益", "目標経常利益", "累計経常利益", "目標累積経常利益"]:
            if pdf_sec_cat == "当月経常利益":
                csv_sec_cat_match = "本年実績"
            elif pdf_sec_cat == "目標経常利益":
                csv_sec_cat_match = "本年目標"
            elif pdf_sec_cat == "累計経常利益":
                csv_sec_cat_match = "累積本年実績"
            elif pdf_sec_cat == "目標累積経常利益":
                csv_sec_cat_match = "累積本年目標"

        # CSVから該当行を探す
        csv_match = None
        for _, csv_row in csv_df.iterrows():
            csv_main_cat = normalize_main_category(csv_row.get("main_category", ""))
            csv_sec_cat = normalize_secondary_category(csv_row.get("secondary_category", ""))

            if csv_main_cat == pdf_main_cat and csv_sec_cat == csv_sec_cat_match:
                csv_match = csv_row
                break

        if csv_match is None:
            # CSVで該当行が見つからない
            continue

        # PDF の各部門の値を比較
        pdf_columns = [c for c in pdf_df.columns if c not in ["区分（大）", "区分（小）"]]

        for pdf_dept in pdf_columns:
            # PDFの値を取得
            pdf_val = parse_number(pdf_row.get(pdf_dept, None))

            # CSVの対応する部門を探す
            csv_dept = normalize_department_name(pdf_dept)

            csv_val = None
            if csv_dept in csv_match.index:
                csv_val = csv_match[csv_dept]

            # 利益率の場合は%表記とdecimal表記の変換
            if pdf_main_cat in ["売上総利益率"] and pdf_val is not None:
                # PDFは%表記（例: 10.90）、CSVはdecimal（例: 0.109）
                # CSVの値を100倍して比較
                if csv_val is not None and not pd.isna(csv_val):
                    try:
                        csv_val = float(csv_val) * 100
                    except (ValueError, TypeError):
                        csv_val = None
            else:
                if csv_val is not None and not pd.isna(csv_val):
                    try:
                        csv_val = float(csv_val)
                    except (ValueError, TypeError):
                        csv_val = None

            # 比較
            if pdf_val is None and csv_val is None:
                continue

            diff = None
            is_equal = 0
            is_large_diff = 0

            if pdf_val is not None and csv_val is not None:
                diff = pdf_val - csv_val
                # 小数点以下の誤差を考慮
                if abs(diff) < 1:
                    is_equal = 1
                if abs(diff) >= 6:
                    is_large_diff = 1
            elif pdf_val is not None and csv_val is None:
                diff = pdf_val
                is_large_diff = 1 if abs(pdf_val) >= 6 else 0
            elif pdf_val is None and csv_val is not None:
                diff = -csv_val
                is_large_diff = 1 if abs(csv_val) >= 6 else 0

            results.append({
                "main_department": branch,
                "year_month": month,
                "main_category": pdf_main_cat,
                "secondary_category": pdf_sec_cat,
                "secondary_department": pdf_dept,
                "pdf_val": pdf_val,
                "csv_val": csv_val,
                "diff_pdf_csv": diff,
                "is_equal": is_equal,
                "is_large_diff": is_large_diff,
                "invest_result": None
            })

    return results


def main():
    """メイン処理"""
    timestamp = datetime.now().strftime("%Y%m%d%H%M")
    all_results = []

    for branch in BRANCHES:
        for month in MONTHS:
            print(f"Comparing {branch} - {month}...")
            results = compare_branch_month(branch, month)
            all_results.extend(results)

            # 支店・月ごとにCSV出力
            if results:
                output_dir = PROJECT_ROOT / "docs" / "test" / branch / month
                output_dir.mkdir(parents=True, exist_ok=True)
                output_path = output_dir / f"test_output_{timestamp}.csv"

                df = pd.DataFrame(results)
                df.to_csv(output_path, index=False, encoding="utf-8-sig")
                print(f"  -> Saved to {output_path}")

    # 全体結果を出力
    if all_results:
        all_output_path = PROJECT_ROOT / "docs" / "test" / f"test_output_all_{timestamp}.csv"
        all_df = pd.DataFrame(all_results)
        all_df.to_csv(all_output_path, index=False, encoding="utf-8-sig")
        print(f"\nAll results saved to {all_output_path}")

        # 不一致の項目を表示
        mismatched = all_df[all_df["is_equal"] == 0]
        print(f"\n=== 不一致項目 (is_equal=0): {len(mismatched)} 件 ===")

        # 大きな差分の項目を表示
        large_diff = all_df[all_df["is_large_diff"] == 1]
        print(f"\n=== 大きな差分 (is_large_diff=1): {len(large_diff)} 件 ===")
        if not large_diff.empty:
            # 支店・月ごとにサマリーを表示
            summary = large_diff.groupby(["main_department", "year_month", "main_category"]).size().reset_index(name="count")
            print(summary.to_string())


if __name__ == "__main__":
    main()
