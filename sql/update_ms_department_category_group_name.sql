/*
============================================================
ms_department_category テーブルにgroup_nameカラムを追加
============================================================
目的: 営業経費などの集計で使用する集計グループ名を管理

実行手順:
1. カラム追加
2. データ更新
3. 確認

注意: 必ず順番に実行してください
============================================================
*/

-- ============================================================
-- 1. カラム追加
-- ============================================================
ALTER TABLE `data-platform-prod-475201.corporate_data.ms_department_category`
ADD COLUMN group_name STRING;


-- ============================================================
-- 2. データ更新（集計グループの定義）
-- ============================================================
UPDATE `data-platform-prod-475201.corporate_data.ms_department_category`
SET group_name = CASE
  -- ガラス工事計: 工事営業１課(11) + 業務課(18)
  WHEN department_category_code IN (11, 18) THEN 'ガラス工事計'

  -- 山本（改装）: 改修課(13)
  WHEN department_category_code = 13 THEN '山本（改装）'

  -- 硝子建材営業部: 硝子建材営業課(20) または 硝子建材営業部(62)
  WHEN department_category_code IN (20, 62) THEN '硝子建材営業部'

  -- その他の部門はNULL（集計対象外）
  ELSE NULL
END
WHERE TRUE;


-- ============================================================
-- 3. 確認クエリ
-- ============================================================
SELECT
  department_category_code,
  department_category_code_name,
  group_name
FROM `data-platform-prod-475201.corporate_data.ms_department_category`
WHERE group_name IS NOT NULL
ORDER BY
  CASE group_name
    WHEN 'ガラス工事計' THEN 1
    WHEN '山本（改装）' THEN 2
    WHEN '硝子建材営業部' THEN 3
  END,
  department_category_code;

/*
期待される結果:
+--------------------------+-------------------------------+--------------------+
| department_category_code | department_category_code_name | group_name         |
+--------------------------+-------------------------------+--------------------+
|                       11 | 工事営業１課                  | ガラス工事計       |
|                       18 | 業務課                        | ガラス工事計       |
|                       13 | 改修課                        | 山本（改装）       |
|                       20 | 硝子建材営業課                | 硝子建材営業部     |
|                       62 | 硝子建材営業部                | 硝子建材営業部     |
+--------------------------+-------------------------------+--------------------+
*/
