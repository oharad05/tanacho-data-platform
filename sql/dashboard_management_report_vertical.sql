/*
============================================================
経営資料（当月）ダッシュボード用SQL（縦持ち形式）
============================================================
目的: 月次損益計算書を組織階層別に可視化（Looker Studio用縦持ち出力）
対象データ: 前月実績データ
組織階層: 東京支店計 > 工事営業部計/硝子建材営業部 > 担当者別/部門別

出力スキーマ:
  - date: 対象月（DATE型）
  - main_category: 大項目（売上高、売上総利益など）
  - secondary_category: 小項目（前年実績、本年目標、本年実績、またはNULL）
  - main_department: 最上位部門（東京支店）
  - secondary_department: 詳細部門（東京支店計、工事営業部計、佐々木（大成・鹿島他）など）
  - value: 集計値

使用方法:
  DECLARE target_month DATE DEFAULT '2025-09-01';  -- 対象月を設定

注意事項:
  - 金額は円単位でDBに格納、Looker Studioで千円表示
  - 売上総利益率は小数（0.3 = 30%）で格納
============================================================
*/

/*
============================================================
■クエリ時作成メモ
・先方に確認が必要な事項はクエリ内に「要確認」とコメント
・前年実績はデータを取り込んでいないため､後回し
■要確認(全体)
・「1行目C列以降の値が一致したものを、代入する。」と書かれている部分について､詳しく伺いたいです｡
 - どのファイルの1行目C列以降なのか?
 - ロジック自体は本年実績と同じで取得対象を前年にするイメージだが､それだと問題があるか?
 ・「営業外費用（社内利息A・B）」について､「山本（改装）」だけが前月を参照しているのは正しいでしょうか?理由を教えて頂けないでしょうか?
 ・スプレッドシートに加工ロジックを書いて頂いておりますが､この計算を行っているエクセルファイルなどはございますか?(もしあれば､整合性検証の際にもそちらと比較すると良いと考えております｡)

■todo
・前年実績を取得する処理 → ✅完了（sales_actual_prev_yearで1年前のデータを取得）
・千円→円の変換 → ✅完了（profit_plan_dataで実装済み）
・"売上総利益の前年実績-営業経費の前年実績※各列ごとに計算" →これが引き算になっていないので修正
・雑損失の計算式がなさそう → ✅完了（miscellaneous_loss CTEで実装、ledger_lossから取得）
・glass_interestはどの項目の集計を行っている?
============================================================
*/

-- ============================================================
-- パラメータ設定
-- ============================================================
DECLARE target_month DATE DEFAULT '2025-09-01';  -- 対象月（前月）
DECLARE target_month_prev_year DATE DEFAULT DATE_SUB(target_month, INTERVAL 1 YEAR);  -- 前年同月
DECLARE fiscal_year_start DATE DEFAULT '2025-04-01';  -- 期首（会計年度開始月）
DECLARE two_months_ago DATE DEFAULT DATE_SUB(target_month, INTERVAL 1 MONTH);  -- 社内利息計算用


-- ============================================================
-- 1. 売上高・粗利実績（本年実績）
-- ============================================================
WITH sales_actual AS (
  SELECT
    -- 組織識別
    CASE
      WHEN branch_code = 11 THEN '工事営業部'
      WHEN branch_code = 25 THEN '硝子建材営業部'
      ELSE 'その他'
    END AS organization,
    -- 担当者・部門別分類
    CASE
      -- 工事営業部の担当者別
      WHEN branch_code = 11 AND staff_name = '佐々木' THEN '佐々木（大成・鹿島他）'
      WHEN branch_code = 11 AND staff_name = '岡本' THEN '岡本（清水他）'
      WHEN branch_code = 11 AND staff_name = '小笠原' THEN '小笠原（三井住友他）'
      WHEN branch_code = 11 AND staff_name = '高石' THEN '高石（内装・リニューアル）'
      WHEN branch_code = 11 AND staff_name = '山本' THEN '山本（改装）'
      -- 硝子建材営業部の部門別
      WHEN branch_code = 25 AND division_code = 11 THEN '硝子工事'
      WHEN branch_code = 25 AND division_code = 21 THEN 'ビルサッシ'
      WHEN branch_code = 25 AND division_code = 10 THEN '硝子販売'
      WHEN branch_code = 25 AND division_code = 20 THEN 'サッシ販売'
      WHEN branch_code = 25 AND division_code = 22 THEN 'サッシ完成品'
      WHEN branch_code = 25 AND division_code IN (12, 23, 24, 25, 30, 31, 40, 41, 50, 70, 71, 99) THEN 'その他'

      ELSE '未分類'
    END AS detail_category,
    -- 金額（円単位）
    SUM(sales_actual) AS sales_amount,
    --要確認: 1_全支店[1.売上管理] 担当者売上目標／実績データ の金額は千円単位→円単位の変換は不要か?(金額的にした方が良さそうだが､該当データソースに詳細コメントがないため､そのまま連携)
    SUM(case when branch_code = 11 then gross_profit_actual else null end) AS gross_profit_amount
    --要確認: 「B列「営業所コード」が"011"かつ、」と書いてあるので､売上純利益のみ営業所コードでフィルタしているが､問題ないか?
    --要確認: 売上高と純利益について､売上高は浅井（清水他）：担当者＝浅井,売上純利益は岡本（清水他）：担当者＝岡本になっているが､この差分は正しいか?今後人の増減が発生した際にcase whenで運用するのは望ましくないので､管理用のマスタファイルを作った方が良さそう｡

  FROM
    `data-platform-prod-475201.corporate_data.sales_target_and_achievements`
  WHERE
    sales_accounting_period = target_month
    AND branch_code IN (11, 25)  -- 営業所コード: 011=工事営業部, 025=硝子建材営業部
    --要確認: 東京支店計＝工事営業部計＋硝子建材部計なので他のbranch_codeは無視してよいか
  GROUP BY
    organization,
    detail_category
)
,

-- ============================================================
-- 1-2. 売上高・粗利実績（前年実績）
-- ============================================================
sales_actual_prev_year AS (
  SELECT
    -- 組織識別
    CASE
      WHEN branch_code = 11 THEN '工事営業部'
      WHEN branch_code = 25 THEN '硝子建材営業部'
      ELSE 'その他'
    END AS organization,
    -- 担当者・部門別分類
    CASE
      -- 工事営業部の担当者別
      WHEN branch_code = 11 AND staff_name = '佐々木' THEN '佐々木（大成・鹿島他）'
      WHEN branch_code = 11 AND staff_name = '岡本' THEN '岡本（清水他）'
      WHEN branch_code = 11 AND staff_name = '小笠原' THEN '小笠原（三井住友他）'
      WHEN branch_code = 11 AND staff_name = '高石' THEN '高石（内装・リニューアル）'
      WHEN branch_code = 11 AND staff_name = '山本' THEN '山本（改装）'
      -- 硝子建材営業部の部門別
      WHEN branch_code = 25 AND division_code = 11 THEN '硝子工事'
      WHEN branch_code = 25 AND division_code = 21 THEN 'ビルサッシ'
      WHEN branch_code = 25 AND division_code = 10 THEN '硝子販売'
      WHEN branch_code = 25 AND division_code = 20 THEN 'サッシ販売'
      WHEN branch_code = 25 AND division_code = 22 THEN 'サッシ完成品'
      WHEN branch_code = 25 AND division_code IN (12, 23, 24, 25, 30, 31, 40, 41, 50, 70, 71, 99) THEN 'その他'

      ELSE '未分類'
    END AS detail_category,
    -- 金額（円単位）
    SUM(sales_actual) AS sales_amount,
    SUM(case when branch_code = 11 then gross_profit_actual else null end) AS gross_profit_amount

  FROM
    `data-platform-prod-475201.corporate_data.sales_target_and_achievements`
  WHERE
    sales_accounting_period = target_month_prev_year  -- 前年同月のデータを取得
    AND branch_code IN (11, 25)  -- 営業所コード: 011=工事営業部, 025=硝子建材営業部
  GROUP BY
    organization,
    detail_category
)
,

-- ============================================================
-- 2. 売上高・粗利目標
-- ============================================================
profit_plan_data AS (
  SELECT
    period,
    item,  -- 項目（売上高、売上総利益、経常利益）
    -- 各組織の金額を展開（千円→円単位に変換）
    tokyo_branch_total * 1000 AS tokyo_branch_total,
    construction_sales_department_total * 1000 AS construction_sales_dept_total,
    company_sasaki * 1000 AS sasaki_amount,
    company_asai * 1000 AS asai_amount,
    company_ogasawara * 1000 AS ogasawara_amount,
    company_takaishi * 1000 AS takaishi_amount,
    company_yamamoto * 1000 AS yamamoto_amount,
    glass_building_material_sales_department * 1000 AS glass_material_dept_total,
    glass_construction * 1000 AS glass_construction_amount,
    building_sash * 1000 AS building_sash_amount,
    glass_sales * 1000 AS glass_sales_amount,
    sash_sales * 1000 AS sash_sales_amount,
    sash_finished_products * 1000 AS sash_finished_amount,
    others * 1000 AS others_amount

  FROM
    `data-platform-prod-475201.corporate_data.profit_plan_term`
  WHERE
    period = target_month
),

-- 本年目標
sales_target AS (
  SELECT
    'sales' AS metric_type,
    '本年目標' AS period_type,
    *
  FROM profit_plan_data
  WHERE item = '売上高'
),

gross_profit_target AS (
  SELECT
    'gross_profit' AS metric_type,
    '本年目標' AS period_type,
    *
  FROM profit_plan_data
  WHERE item = '売上総利益'
),


-- ============================================================
-- 3. 営業経費（本年実績）
-- ============================================================
operating_expenses AS (
  SELECT
    CASE
      WHEN code IN ('11', '18') THEN 'ガラス工事計'
      WHEN code = '13' THEN '山本（改装）'
      WHEN code = '20' THEN '硝子建材営業部'
      ELSE '未分類'
    END AS detail_category,

    -- 経費科目の合計（円単位）
    SUM(
      CASE
        WHEN subject_name IN (
          '8331', '8333', '8334', '8335', '8338',
          '8340', '8341', '8342', '8343', '8344', '8345', '8346', '8347',
          '8349', '8350', '8351', '8352', '8353', '8354', '8355', '8356',
          '8357', '8358', '8359', '8361'
        ) THEN total
        ELSE 0
      END
    ) AS operating_expense_amount

  FROM
    `data-platform-prod-475201.corporate_data.department_summary`
  WHERE
    sales_accounting_period = target_month
    AND code IN ('11', '18', '13', '20')
  GROUP BY
    detail_category
),


-- ============================================================
-- 4. 営業外収入（リベート・その他）
-- ============================================================
non_operating_income AS (
  SELECT
    -- 部門コードから組織を判定
    CASE
      WHEN own_department_code = 11 THEN 'ガラス工事計'
      WHEN own_department_code in (13, 18) THEN '山本（改装）'
      WHEN own_department_code = 20 THEN '硝子建材営業部'
      ELSE '未分類'
    END AS detail_category,

    -- リベート収入（全角・半角両方判定）
    SUM(
      CASE
        WHEN REGEXP_CONTAINS(description_comment, r'リベート|リベート')
        THEN amount
        ELSE 0
      END
    ) AS rebate_income,

    -- その他営業外収入
    SUM(
      CASE
        WHEN NOT REGEXP_CONTAINS(description_comment, r'リベート|リベート')
        THEN amount
        ELSE 0
      END
    ) AS other_non_operating_income

  FROM
    `data-platform-prod-475201.corporate_data.ledger_income`
  WHERE
    DATE(slip_date) = target_month
    AND own_department_code IN (11, 13, 18, 20)
  GROUP BY
    detail_category
),

-- ============================================================
-- 5. 営業外費用（社内利息）
-- ============================================================
-- 5-1. 山本（改装）の社内利息計算
yamamoto_interest AS (
  SELECT
    '山本（改装）' AS detail_category,

    -- 売掛残高 × 利率
    bb.current_month_sales_balance * ii.interest_rate AS interest_expense

  FROM
    `data-platform-prod-475201.corporate_data.billing_balance` AS bb
  inner JOIN
  # 要確認: join条件は日付のみ?
    `data-platform-prod-475201.corporate_data.internal_interest` AS ii
  on bb.sales_month = ii.year_month
  WHERE
    bb.sales_month = two_months_ago  -- 2か月前のデータ
    # 要確認: 「売上月度」が実行日の２か月前と書いてあるが､参照先は前月(2025-08)?作成日と実行日は別の意味?
    AND bb.branch_code = 13  -- 営業所コード013
    AND ii.year_month = two_months_ago
    AND ii.branch = '東京支店'
    AND ii.category = '売掛金'
  LIMIT 1
),

-- 5-2. 部門集計表からの社内利息
department_interest AS (
  SELECT
    CASE
      WHEN code = '11' THEN 'ガラス工事計'
      WHEN code = '20' THEN '硝子建材営業部'
      ELSE '未分類'
    END AS detail_category,

    SUM(total) AS interest_from_summary

  FROM
    `data-platform-prod-475201.corporate_data.department_summary`
  WHERE
    sales_accounting_period = target_month
    AND subject_name = '9250'  -- 社内利息の勘定科目コード
    AND code IN ('11', '20')
  GROUP BY
    detail_category
),

-- 5-3. ガラス工事計の社内利息（山本分を除く）
glass_interest AS (
  SELECT
    'ガラス工事計' AS detail_category,
    di.interest_from_summary - COALESCE(yi.interest_expense, 0) AS interest_expense
  FROM
    department_interest di
  LEFT JOIN
    yamamoto_interest yi
  ON di.detail_category = 'ガラス工事計'
  WHERE
    di.detail_category = 'ガラス工事計'
),

-- 統合
non_operating_expenses AS (
  SELECT detail_category, interest_expense FROM yamamoto_interest
  UNION ALL
  SELECT detail_category, interest_expense FROM glass_interest
  UNION ALL
  SELECT detail_category, interest_from_summary AS interest_expense
  FROM department_interest
  WHERE detail_category = '硝子建材営業部'
),

-- ============================================================
-- 6. 営業外費用（雑損失）
-- ============================================================
miscellaneous_loss AS (
  SELECT
    -- 部門コードから組織を判定
    CASE
      WHEN own_department_code IN (11, 18) THEN 'ガラス工事計'
      WHEN own_department_code = 13 THEN '山本（改装）'
      WHEN own_department_code = 20 THEN '硝子建材営業部'
      ELSE '未分類'
    END AS detail_category,

    -- 雑損失金額
    SUM(amount) AS miscellaneous_loss_amount

  FROM
    `data-platform-prod-475201.corporate_data.ledger_loss`
  WHERE
    DATE(slip_date) = target_month
    AND own_department_code IN (11, 13, 18, 20)
  GROUP BY
    detail_category
),

-- ============================================================
-- 7. 本店管理費
-- ============================================================
head_office_expenses AS (
  SELECT
    CASE
      WHEN code = '11' THEN 'ガラス工事計'
      WHEN code = '13' THEN '山本（改装）'
      WHEN code = '20' THEN '硝子建材営業部'
      ELSE '未分類'
    END AS detail_category,

    SUM(total) AS head_office_expense

  FROM
    `data-platform-prod-475201.corporate_data.department_summary`
  WHERE
    sales_accounting_period = target_month
    AND subject_name = '8366'  -- 本店管理費の勘定科目コード
    AND code IN ('11', '13', '20')
  GROUP BY
    detail_category
),


-- ============================================================
-- 8. 経常利益目標
-- ============================================================
recurring_profit_target AS (
  SELECT
    'recurring_profit' AS metric_type,
    '本年目標' AS period_type,
    *
  FROM profit_plan_data
  WHERE item = '経常利益'
),


-- ============================================================
-- 9. 全指標の統合
-- ============================================================
consolidated_metrics AS (
  SELECT
    sa.organization,
    sa.detail_category,

    -- ========== 売上高 ==========
    sa.sales_amount AS sales_actual,  -- 本年実績
    NULL AS sales_target,  -- 本年目標（後で結合）
    COALESCE(sa_prev.sales_amount, 0) AS sales_prev_year,  -- 前年実績

    -- ========== 売上総利益 ==========
    sa.gross_profit_amount AS gross_profit_actual,  -- 本年実績
    NULL AS gross_profit_target,  -- 本年目標
    COALESCE(sa_prev.gross_profit_amount, 0) AS gross_profit_prev_year,  -- 前年実績

    -- ========== 売上総利益率 ==========
    SAFE_DIVIDE(sa.gross_profit_amount, sa.sales_amount) AS gross_profit_margin_actual,
    NULL AS gross_profit_margin_target,
    SAFE_DIVIDE(sa_prev.gross_profit_amount, sa_prev.sales_amount) AS gross_profit_margin_prev_year,

    -- ========== 営業経費 ==========
    COALESCE(oe.operating_expense_amount, 0) AS operating_expense_actual,
    NULL AS operating_expense_target,
    NULL AS operating_expense_prev_year,

    -- ========== 営業利益 ==========
    sa.gross_profit_amount - COALESCE(oe.operating_expense_amount, 0) AS operating_income_actual,
    NULL AS operating_income_target,
    NULL AS operating_income_prev_year,

    -- ========== 営業外収入 ==========
    COALESCE(noi.rebate_income, 0) AS rebate_income,
    COALESCE(noi.other_non_operating_income, 0) AS other_non_operating_income,

    -- ========== 営業外費用 ==========
    COALESCE(noe.interest_expense, 0) AS non_operating_expenses,
    COALESCE(ml.miscellaneous_loss_amount, 0) AS miscellaneous_loss,

    -- ========== 本店管理費 ==========
    COALESCE(hoe.head_office_expense, 0) AS head_office_expense,

    -- ========== 経常利益 ==========
    (
      sa.gross_profit_amount
      - COALESCE(oe.operating_expense_amount, 0)
      + COALESCE(noi.rebate_income, 0)
      + COALESCE(noi.other_non_operating_income, 0)
      - COALESCE(noe.interest_expense, 0)
      - COALESCE(ml.miscellaneous_loss_amount, 0)
      - COALESCE(hoe.head_office_expense, 0)
    ) AS recurring_profit_actual,
    NULL AS recurring_profit_target

  FROM
    sales_actual sa
  LEFT JOIN
    sales_actual_prev_year sa_prev
    ON sa.organization = sa_prev.organization
    AND sa.detail_category = sa_prev.detail_category
  LEFT JOIN
    operating_expenses oe
    ON sa.detail_category = oe.detail_category
  LEFT JOIN
    non_operating_income noi
    ON sa.detail_category = noi.detail_category
  LEFT JOIN
    non_operating_expenses noe
    ON sa.detail_category = noe.detail_category
  LEFT JOIN
    miscellaneous_loss ml
    ON sa.detail_category = ml.detail_category
  LEFT JOIN
    head_office_expenses hoe
    ON sa.detail_category = hoe.detail_category
),


-- ============================================================
-- 10. 組織階層の集計（工事営業部計、東京支店計）
-- ============================================================
aggregated_metrics AS (
  -- 詳細レベル（担当者別・部門別）
  SELECT *
  FROM consolidated_metrics

  UNION ALL

  -- 中間レベル（ガラス工事計 = 佐々木+岡本+小笠原+高石）
  SELECT
    organization,
    'ガラス工事計' AS detail_category,
    SUM(sales_actual) AS sales_actual,
    SUM(sales_target) AS sales_target,
    SUM(sales_prev_year) AS sales_prev_year,
    SUM(gross_profit_actual) AS gross_profit_actual,
    SUM(gross_profit_target) AS gross_profit_target,
    SUM(gross_profit_prev_year) AS gross_profit_prev_year,
    SAFE_DIVIDE(SUM(gross_profit_actual), SUM(sales_actual)) AS gross_profit_margin_actual,
    SAFE_DIVIDE(SUM(gross_profit_target), SUM(sales_target)) AS gross_profit_margin_target,
    SAFE_DIVIDE(SUM(gross_profit_prev_year), SUM(sales_prev_year)) AS gross_profit_margin_prev_year,
    SUM(operating_expense_actual) AS operating_expense_actual,
    SUM(operating_expense_target) AS operating_expense_target,
    SUM(operating_expense_prev_year) AS operating_expense_prev_year,
    SUM(operating_income_actual) AS operating_income_actual,
    SUM(operating_income_target) AS operating_income_target,
    SUM(operating_income_prev_year) AS operating_income_prev_year,
    SUM(rebate_income) AS rebate_income,
    SUM(other_non_operating_income) AS other_non_operating_income,
    SUM(non_operating_expenses) AS non_operating_expenses,
    SUM(miscellaneous_loss) AS miscellaneous_loss,
    SUM(head_office_expense) AS head_office_expense,
    SUM(recurring_profit_actual) AS recurring_profit_actual,
    SUM(recurring_profit_target) AS recurring_profit_target
  FROM consolidated_metrics
  WHERE organization = '工事営業部'
    AND detail_category IN ('佐々木（大成・鹿島他）', '岡本（清水他）', '小笠原（三井住友他）', '高石（内装・リニューアル）')
  GROUP BY organization

  UNION ALL

  -- 組織計レベル（工事営業部計、硝子建材営業部計）
  SELECT
    organization,
    CONCAT(organization, '計') AS detail_category,
    SUM(sales_actual) AS sales_actual,
    SUM(sales_target) AS sales_target,
    SUM(sales_prev_year) AS sales_prev_year,
    SUM(gross_profit_actual) AS gross_profit_actual,
    SUM(gross_profit_target) AS gross_profit_target,
    SUM(gross_profit_prev_year) AS gross_profit_prev_year,
    SAFE_DIVIDE(SUM(gross_profit_actual), SUM(sales_actual)) AS gross_profit_margin_actual,
    SAFE_DIVIDE(SUM(gross_profit_target), SUM(sales_target)) AS gross_profit_margin_target,
    SAFE_DIVIDE(SUM(gross_profit_prev_year), SUM(sales_prev_year)) AS gross_profit_margin_prev_year,
    SUM(operating_expense_actual) AS operating_expense_actual,
    SUM(operating_expense_target) AS operating_expense_target,
    SUM(operating_expense_prev_year) AS operating_expense_prev_year,
    SUM(operating_income_actual) AS operating_income_actual,
    SUM(operating_income_target) AS operating_income_target,
    SUM(operating_income_prev_year) AS operating_income_prev_year,
    SUM(rebate_income) AS rebate_income,
    SUM(other_non_operating_income) AS other_non_operating_income,
    SUM(non_operating_expenses) AS non_operating_expenses,
    SUM(miscellaneous_loss) AS miscellaneous_loss,
    SUM(head_office_expense) AS head_office_expense,
    SUM(recurring_profit_actual) AS recurring_profit_actual,
    SUM(recurring_profit_target) AS recurring_profit_target
  FROM consolidated_metrics
  GROUP BY organization

  UNION ALL

  -- 最上位レベル（東京支店計）
  SELECT
    '東京支店' AS organization,
    '東京支店計' AS detail_category,
    SUM(sales_actual) AS sales_actual,
    SUM(sales_target) AS sales_target,
    SUM(sales_prev_year) AS sales_prev_year,
    SUM(gross_profit_actual) AS gross_profit_actual,
    SUM(gross_profit_target) AS gross_profit_target,
    SUM(gross_profit_prev_year) AS gross_profit_prev_year,
    SAFE_DIVIDE(SUM(gross_profit_actual), SUM(sales_actual)) AS gross_profit_margin_actual,
    SAFE_DIVIDE(SUM(gross_profit_target), SUM(sales_target)) AS gross_profit_margin_target,
    SAFE_DIVIDE(SUM(gross_profit_prev_year), SUM(sales_prev_year)) AS gross_profit_margin_prev_year,
    SUM(operating_expense_actual) AS operating_expense_actual,
    SUM(operating_expense_target) AS operating_expense_target,
    SUM(operating_expense_prev_year) AS operating_expense_prev_year,
    SUM(operating_income_actual) AS operating_income_actual,
    SUM(operating_income_target) AS operating_income_target,
    SUM(operating_income_prev_year) AS operating_income_prev_year,
    SUM(rebate_income) AS rebate_income,
    SUM(other_non_operating_income) AS other_non_operating_income,
    SUM(non_operating_expenses) AS non_operating_expenses,
    SUM(miscellaneous_loss) AS miscellaneous_loss,
    SUM(head_office_expense) AS head_office_expense,
    SUM(recurring_profit_actual) AS recurring_profit_actual,
    SUM(recurring_profit_target) AS recurring_profit_target
  FROM consolidated_metrics
)

-- ============================================================
-- 11. 縦持ち形式への変換（UNION ALL）
-- ============================================================
,
vertical_format AS (
  -- 売上高
  SELECT
    target_month AS date,
    '売上高' AS main_category,
    1 AS main_category_sort_order,
    '前年実績' AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    sales_prev_year AS value
  FROM aggregated_metrics
  UNION ALL
  SELECT
    target_month AS date,
    '売上高' AS main_category,
    1 AS main_category_sort_order,
    '本年目標' AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    sales_target AS value
  FROM aggregated_metrics
  UNION ALL
  SELECT
    target_month AS date,
    '売上高' AS main_category,
    1 AS main_category_sort_order,
    '本年実績' AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    sales_actual AS value
  FROM aggregated_metrics

  UNION ALL

  -- 売上総利益
  SELECT
    target_month AS date,
    '売上総利益' AS main_category,
    2 AS main_category_sort_order,
    '前年実績' AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    gross_profit_prev_year AS value
  FROM aggregated_metrics
  UNION ALL
  SELECT
    target_month AS date,
    '売上総利益' AS main_category,
    2 AS main_category_sort_order,
    '本年目標' AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    gross_profit_target AS value
  FROM aggregated_metrics
  UNION ALL
  SELECT
    target_month AS date,
    '売上総利益' AS main_category,
    2 AS main_category_sort_order,
    '本年実績' AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    gross_profit_actual AS value
  FROM aggregated_metrics

  UNION ALL

  -- 売上総利益率
  SELECT
    target_month AS date,
    '売上総利益率' AS main_category,
    3 AS main_category_sort_order,
    '前年実績' AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    gross_profit_margin_prev_year AS value
  FROM aggregated_metrics
  UNION ALL
  SELECT
    target_month AS date,
    '売上総利益率' AS main_category,
    3 AS main_category_sort_order,
    '本年目標' AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    gross_profit_margin_target AS value
  FROM aggregated_metrics
  UNION ALL
  SELECT
    target_month AS date,
    '売上総利益率' AS main_category,
    3 AS main_category_sort_order,
    '本年実績' AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    gross_profit_margin_actual AS value
  FROM aggregated_metrics

  UNION ALL

  -- 営業経費
  SELECT
    target_month AS date,
    '営業経費' AS main_category,
    4 AS main_category_sort_order,
    '前年実績' AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    operating_expense_prev_year AS value
  FROM aggregated_metrics
  UNION ALL
  SELECT
    target_month AS date,
    '営業経費' AS main_category,
    4 AS main_category_sort_order,
    '本年目標' AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    operating_expense_target AS value
  FROM aggregated_metrics
  UNION ALL
  SELECT
    target_month AS date,
    '営業経費' AS main_category,
    4 AS main_category_sort_order,
    '本年実績' AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    operating_expense_actual AS value
  FROM aggregated_metrics

  UNION ALL

  -- 営業利益
  SELECT
    target_month AS date,
    '営業利益' AS main_category,
    5 AS main_category_sort_order,
    '前年実績' AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    operating_income_prev_year AS value
  FROM aggregated_metrics
  UNION ALL
  SELECT
    target_month AS date,
    '営業利益' AS main_category,
    5 AS main_category_sort_order,
    '本年目標' AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    operating_income_target AS value
  FROM aggregated_metrics
  UNION ALL
  SELECT
    target_month AS date,
    '営業利益' AS main_category,
    5 AS main_category_sort_order,
    '本年実績' AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    operating_income_actual AS value
  FROM aggregated_metrics

  UNION ALL

  -- 営業外収入（リベート）
  SELECT
    target_month AS date,
    '営業外収入（リベート）' AS main_category,
    6 AS main_category_sort_order,
    CAST(NULL AS STRING) AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    rebate_income AS value
  FROM aggregated_metrics

  UNION ALL

  -- 営業外収入（その他）
  SELECT
    target_month AS date,
    '営業外収入（その他）' AS main_category,
    7 AS main_category_sort_order,
    CAST(NULL AS STRING) AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    other_non_operating_income AS value
  FROM aggregated_metrics

  UNION ALL

  -- 営業外費用（社内利息A・B）
  SELECT
    target_month AS date,
    '営業外費用（社内利息A・B）' AS main_category,
    8 AS main_category_sort_order,
    CAST(NULL AS STRING) AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    non_operating_expenses AS value
  FROM aggregated_metrics

  UNION ALL

  -- 営業外費用（雑損失）
  SELECT
    target_month AS date,
    '営業外費用（雑損失）' AS main_category,
    9 AS main_category_sort_order,
    CAST(NULL AS STRING) AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    miscellaneous_loss AS value
  FROM aggregated_metrics

  UNION ALL

  -- 本店管理費
  SELECT
    target_month AS date,
    '本店管理費' AS main_category,
    10 AS main_category_sort_order,
    CAST(NULL AS STRING) AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    head_office_expense AS value
  FROM aggregated_metrics

  UNION ALL

  -- 経常利益
  SELECT
    target_month AS date,
    '経常利益' AS main_category,
    11 AS main_category_sort_order,
    '本年目標' AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    recurring_profit_target AS value
  FROM aggregated_metrics
  UNION ALL
  SELECT
    target_month AS date,
    '経常利益' AS main_category,
    11 AS main_category_sort_order,
    '本年実績' AS secondary_category,
    '東京支店' AS main_department,
    detail_category AS secondary_department,
    CASE detail_category
      WHEN '東京支店計' THEN 1
      WHEN '工事営業部計' THEN 2
      WHEN '佐々木（大成・鹿島他）' THEN 3
      WHEN '浅井（竹中・清水他）' THEN 4
      WHEN '岡本（清水他）' THEN 5
      WHEN '岡本（戸田・三井他）' THEN 5
      WHEN '小笠原（三井住友他）' THEN 6
      WHEN '小笠原（大林他）' THEN 6
      WHEN '高石（内装・リニューアル）' THEN 7
      WHEN '高石（長谷工他）' THEN 7
      WHEN 'ガラス工事計' THEN 8
      WHEN '山本（改装）' THEN 9
      WHEN '硝子建材営業部計' THEN 10
      WHEN '硝子建材営業部' THEN 10
      ELSE 99
    END AS secondary_department_sort_order,
    recurring_profit_actual AS value
  FROM aggregated_metrics
)

SELECT *
FROM vertical_format
ORDER BY
  secondary_department_sort_order,
  main_category_sort_order,
  CASE secondary_category
    WHEN '前年実績' THEN 1
    WHEN '本年目標' THEN 2
    WHEN '本年実績' THEN 3
    ELSE 4
  END;
