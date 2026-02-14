-- 恋愛占い・相性診断のUU集計用に ip カラムを追加
-- Supabase ダッシュボードの SQL Editor で実行してください。（1回だけ）
-- 実行後、track/love-fortune と track/compatibility でIPが保存され、管理画面でUUが表示されます。

ALTER TABLE feature_events ADD COLUMN IF NOT EXISTS ip TEXT;
COMMENT ON COLUMN feature_events.ip IS 'UU集計用。x-forwarded-for または remoteAddress';
