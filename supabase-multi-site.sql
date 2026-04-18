-- ===================================================================
-- マルチサイト対応 & minako診断アフィリエイター分析 用の追加マイグレーション
-- Supabase ダッシュボードの SQL Editor で 1回だけ実行してください。
-- ===================================================================
--
-- 目的:
--   - 従来の「マリッジタイプ診断」(marriage-type.com) に加えて
--     新しい「minako診断」(minako.marriage.type) をまとめて記録・分析できるよう
--     各テーブルに site カラムを追加する。
--   - minako診断ではアフィリエイターごとに ?ref=XXX のリンクを配るので
--     affiliate_id カラムを追加して詳細分析できるようにする。
--
-- backward compatibility:
--   site のデフォルトを 'marriage_type16' にしているので
--   既存行はすべて従来サイト扱いのまま継続集計される。
-- ===================================================================

-- page_views
ALTER TABLE page_views ADD COLUMN IF NOT EXISTS site TEXT DEFAULT 'marriage_type16';
ALTER TABLE page_views ADD COLUMN IF NOT EXISTS affiliate_id TEXT;
UPDATE page_views SET site = 'marriage_type16' WHERE site IS NULL;
CREATE INDEX IF NOT EXISTS idx_page_views_site ON page_views (site);
CREATE INDEX IF NOT EXISTS idx_page_views_affiliate_id ON page_views (affiliate_id);
COMMENT ON COLUMN page_views.site IS 'marriage_type16 | minako';
COMMENT ON COLUMN page_views.affiliate_id IS 'minako診断のアフィリエイターID（?ref=XXX）';

-- quiz_progress
ALTER TABLE quiz_progress ADD COLUMN IF NOT EXISTS site TEXT DEFAULT 'marriage_type16';
ALTER TABLE quiz_progress ADD COLUMN IF NOT EXISTS affiliate_id TEXT;
UPDATE quiz_progress SET site = 'marriage_type16' WHERE site IS NULL;
CREATE INDEX IF NOT EXISTS idx_quiz_progress_site ON quiz_progress (site);
CREATE INDEX IF NOT EXISTS idx_quiz_progress_affiliate_id ON quiz_progress (affiliate_id);

-- diagnosis_results
ALTER TABLE diagnosis_results ADD COLUMN IF NOT EXISTS site TEXT DEFAULT 'marriage_type16';
ALTER TABLE diagnosis_results ADD COLUMN IF NOT EXISTS affiliate_id TEXT;
UPDATE diagnosis_results SET site = 'marriage_type16' WHERE site IS NULL;
CREATE INDEX IF NOT EXISTS idx_diagnosis_results_site ON diagnosis_results (site);
CREATE INDEX IF NOT EXISTS idx_diagnosis_results_affiliate_id ON diagnosis_results (affiliate_id);

-- feature_events（minako の LINE遷移クリックもここに入れる）
ALTER TABLE feature_events ADD COLUMN IF NOT EXISTS site TEXT DEFAULT 'marriage_type16';
ALTER TABLE feature_events ADD COLUMN IF NOT EXISTS affiliate_id TEXT;
UPDATE feature_events SET site = 'marriage_type16' WHERE site IS NULL;
CREATE INDEX IF NOT EXISTS idx_feature_events_site ON feature_events (site);
CREATE INDEX IF NOT EXISTS idx_feature_events_affiliate_id ON feature_events (affiliate_id);

-- ===================================================================
-- 実行後の確認（任意）
--   SELECT site, COUNT(*) FROM page_views GROUP BY site;
--   SELECT site, COUNT(*) FROM diagnosis_results GROUP BY site;
-- ===================================================================
