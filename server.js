/**
 * マリッジタイプ診断 - 管理用バックエンドサーバー
 */

const express = require('express');
const cors = require('cors');
const Database = require('better-sqlite3');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3001;

// データベース初期化
const db = new Database(path.join(__dirname, 'data.db'));

// テーブル作成
db.exec(`
    CREATE TABLE IF NOT EXISTS page_views (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        page TEXT NOT NULL,
        user_agent TEXT,
        referrer TEXT,
        ip TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS diagnosis_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type_code TEXT NOT NULL,
        type_name TEXT NOT NULL,
        scores TEXT,
        user_agent TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_page_views_created_at ON page_views(created_at);
    CREATE INDEX IF NOT EXISTS idx_diagnosis_results_created_at ON diagnosis_results(created_at);
    CREATE INDEX IF NOT EXISTS idx_diagnosis_results_type_code ON diagnosis_results(type_code);
`);

// ミドルウェア
app.use(cors());
app.use(express.json());

// 静的ファイル配信
app.use('/admin', express.static(path.join(__dirname, 'admin-panel')));

// ヘルスチェック（Render用）
app.get('/api/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// ==========================================
// API: データ記録
// ==========================================

// ページビュー記録
app.post('/api/track/pageview', (req, res) => {
    try {
        const { page } = req.body;
        const userAgent = req.headers['user-agent'] || '';
        const referrer = req.headers['referer'] || '';
        const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress || '';
        
        const stmt = db.prepare(`
            INSERT INTO page_views (page, user_agent, referrer, ip)
            VALUES (?, ?, ?, ?)
        `);
        stmt.run(page, userAgent, referrer, ip);
        
        res.json({ success: true });
    } catch (error) {
        console.error('Error tracking pageview:', error);
        res.status(500).json({ error: 'Failed to track pageview' });
    }
});

// 診断結果記録
app.post('/api/track/diagnosis', (req, res) => {
    try {
        const { typeCode, typeName, scores } = req.body;
        const userAgent = req.headers['user-agent'] || '';
        
        const stmt = db.prepare(`
            INSERT INTO diagnosis_results (type_code, type_name, scores, user_agent)
            VALUES (?, ?, ?, ?)
        `);
        stmt.run(typeCode, typeName, JSON.stringify(scores), userAgent);
        
        res.json({ success: true });
    } catch (error) {
        console.error('Error tracking diagnosis:', error);
        res.status(500).json({ error: 'Failed to track diagnosis' });
    }
});

// ==========================================
// API: 統計データ取得（管理画面用）
// ==========================================

// 管理者認証ミドルウェア（シンプルなAPIキー方式）
const ADMIN_API_KEY = process.env.ADMIN_API_KEY || 'marriage-type-admin-2026';

const adminAuth = (req, res, next) => {
    const apiKey = req.headers['x-api-key'];
    if (apiKey !== ADMIN_API_KEY) {
        return res.status(401).json({ error: 'Unauthorized' });
    }
    next();
};

// ダッシュボード概要
app.get('/api/admin/dashboard', adminAuth, (req, res) => {
    try {
        // 今日の日付
        const today = new Date().toISOString().split('T')[0];
        
        // 総アクセス数
        const totalViews = db.prepare('SELECT COUNT(*) as count FROM page_views').get();
        
        // 今日のアクセス数
        const todayViews = db.prepare(`
            SELECT COUNT(*) as count FROM page_views 
            WHERE DATE(created_at) = DATE(?)
        `).get(today);
        
        // 総診断数
        const totalDiagnosis = db.prepare('SELECT COUNT(*) as count FROM diagnosis_results').get();
        
        // 今日の診断数
        const todayDiagnosis = db.prepare(`
            SELECT COUNT(*) as count FROM diagnosis_results 
            WHERE DATE(created_at) = DATE(?)
        `).get(today);
        
        // タイプ別診断数
        const typeStats = db.prepare(`
            SELECT type_code, type_name, COUNT(*) as count 
            FROM diagnosis_results 
            GROUP BY type_code 
            ORDER BY count DESC
        `).all();
        
        // 過去7日間のアクセス推移
        const dailyViews = db.prepare(`
            SELECT DATE(created_at) as date, COUNT(*) as count 
            FROM page_views 
            WHERE created_at >= DATE('now', '-7 days')
            GROUP BY DATE(created_at)
            ORDER BY date
        `).all();
        
        // 過去7日間の診断数推移
        const dailyDiagnosis = db.prepare(`
            SELECT DATE(created_at) as date, COUNT(*) as count 
            FROM diagnosis_results 
            WHERE created_at >= DATE('now', '-7 days')
            GROUP BY DATE(created_at)
            ORDER BY date
        `).all();
        
        res.json({
            totalViews: totalViews.count,
            todayViews: todayViews.count,
            totalDiagnosis: totalDiagnosis.count,
            todayDiagnosis: todayDiagnosis.count,
            typeStats,
            dailyViews,
            dailyDiagnosis
        });
    } catch (error) {
        console.error('Error getting dashboard data:', error);
        res.status(500).json({ error: 'Failed to get dashboard data' });
    }
});

// 最新の診断結果一覧
app.get('/api/admin/diagnosis/recent', adminAuth, (req, res) => {
    try {
        const limit = parseInt(req.query.limit) || 50;
        
        const results = db.prepare(`
            SELECT id, type_code, type_name, created_at 
            FROM diagnosis_results 
            ORDER BY created_at DESC 
            LIMIT ?
        `).all(limit);
        
        res.json(results);
    } catch (error) {
        console.error('Error getting recent diagnosis:', error);
        res.status(500).json({ error: 'Failed to get recent diagnosis' });
    }
});

// ==========================================
// API: 高度な分析機能
// ==========================================

// 期間指定ダッシュボード
app.get('/api/admin/analytics', adminAuth, (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = startDate || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
        const end = endDate || new Date().toISOString().split('T')[0];
        
        // 期間内のアクセス数
        const periodViews = db.prepare(`
            SELECT COUNT(*) as count FROM page_views 
            WHERE DATE(created_at) BETWEEN DATE(?) AND DATE(?)
        `).get(start, end);
        
        // 期間内の診断数
        const periodDiagnosis = db.prepare(`
            SELECT COUNT(*) as count FROM diagnosis_results 
            WHERE DATE(created_at) BETWEEN DATE(?) AND DATE(?)
        `).get(start, end);
        
        // 日別アクセス推移
        const dailyViews = db.prepare(`
            SELECT DATE(created_at) as date, COUNT(*) as count 
            FROM page_views 
            WHERE DATE(created_at) BETWEEN DATE(?) AND DATE(?)
            GROUP BY DATE(created_at)
            ORDER BY date
        `).all(start, end);
        
        // 日別診断推移
        const dailyDiagnosis = db.prepare(`
            SELECT DATE(created_at) as date, COUNT(*) as count 
            FROM diagnosis_results 
            WHERE DATE(created_at) BETWEEN DATE(?) AND DATE(?)
            GROUP BY DATE(created_at)
            ORDER BY date
        `).all(start, end);
        
        // タイプ別統計（期間内）
        const typeStats = db.prepare(`
            SELECT type_code, type_name, COUNT(*) as count 
            FROM diagnosis_results 
            WHERE DATE(created_at) BETWEEN DATE(?) AND DATE(?)
            GROUP BY type_code 
            ORDER BY count DESC
        `).all(start, end);
        
        res.json({
            period: { start, end },
            periodViews: periodViews.count,
            periodDiagnosis: periodDiagnosis.count,
            dailyViews,
            dailyDiagnosis,
            typeStats
        });
    } catch (error) {
        console.error('Error getting analytics:', error);
        res.status(500).json({ error: 'Failed to get analytics' });
    }
});

// 時間帯別分析
app.get('/api/admin/analytics/hourly', adminAuth, (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = startDate || new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
        const end = endDate || new Date().toISOString().split('T')[0];
        
        // 時間帯別アクセス
        const hourlyViews = db.prepare(`
            SELECT strftime('%H', created_at) as hour, COUNT(*) as count 
            FROM page_views 
            WHERE DATE(created_at) BETWEEN DATE(?) AND DATE(?)
            GROUP BY hour
            ORDER BY hour
        `).all(start, end);
        
        // 時間帯別診断
        const hourlyDiagnosis = db.prepare(`
            SELECT strftime('%H', created_at) as hour, COUNT(*) as count 
            FROM diagnosis_results 
            WHERE DATE(created_at) BETWEEN DATE(?) AND DATE(?)
            GROUP BY hour
            ORDER BY hour
        `).all(start, end);
        
        res.json({ hourlyViews, hourlyDiagnosis });
    } catch (error) {
        console.error('Error getting hourly analytics:', error);
        res.status(500).json({ error: 'Failed to get hourly analytics' });
    }
});

// 曜日別分析
app.get('/api/admin/analytics/weekday', adminAuth, (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = startDate || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
        const end = endDate || new Date().toISOString().split('T')[0];
        
        // 曜日別アクセス (0=日曜, 6=土曜)
        const weekdayViews = db.prepare(`
            SELECT strftime('%w', created_at) as weekday, COUNT(*) as count 
            FROM page_views 
            WHERE DATE(created_at) BETWEEN DATE(?) AND DATE(?)
            GROUP BY weekday
            ORDER BY weekday
        `).all(start, end);
        
        // 曜日別診断
        const weekdayDiagnosis = db.prepare(`
            SELECT strftime('%w', created_at) as weekday, COUNT(*) as count 
            FROM diagnosis_results 
            WHERE DATE(created_at) BETWEEN DATE(?) AND DATE(?)
            GROUP BY weekday
            ORDER BY weekday
        `).all(start, end);
        
        res.json({ weekdayViews, weekdayDiagnosis });
    } catch (error) {
        console.error('Error getting weekday analytics:', error);
        res.status(500).json({ error: 'Failed to get weekday analytics' });
    }
});

// デバイス分析（User-Agentから推定）
app.get('/api/admin/analytics/devices', adminAuth, (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = startDate || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
        const end = endDate || new Date().toISOString().split('T')[0];
        
        const allUserAgents = db.prepare(`
            SELECT user_agent FROM page_views 
            WHERE DATE(created_at) BETWEEN DATE(?) AND DATE(?)
        `).all(start, end);
        
        const devices = { mobile: 0, tablet: 0, desktop: 0 };
        
        allUserAgents.forEach(row => {
            const ua = (row.user_agent || '').toLowerCase();
            if (/ipad|tablet|playbook|silk/.test(ua)) {
                devices.tablet++;
            } else if (/mobile|iphone|ipod|android|blackberry|opera mini|iemobile/.test(ua)) {
                devices.mobile++;
            } else {
                devices.desktop++;
            }
        });
        
        const total = devices.mobile + devices.tablet + devices.desktop;
        
        res.json({
            devices,
            percentages: {
                mobile: total ? Math.round(devices.mobile / total * 100) : 0,
                tablet: total ? Math.round(devices.tablet / total * 100) : 0,
                desktop: total ? Math.round(devices.desktop / total * 100) : 0
            }
        });
    } catch (error) {
        console.error('Error getting device analytics:', error);
        res.status(500).json({ error: 'Failed to get device analytics' });
    }
});

// 流入元分析
app.get('/api/admin/analytics/referrers', adminAuth, (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = startDate || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
        const end = endDate || new Date().toISOString().split('T')[0];
        
        const allReferrers = db.prepare(`
            SELECT referrer FROM page_views 
            WHERE DATE(created_at) BETWEEN DATE(?) AND DATE(?)
            AND referrer IS NOT NULL AND referrer != ''
        `).all(start, end);
        
        const referrerCounts = {};
        
        allReferrers.forEach(row => {
            try {
                const url = new URL(row.referrer);
                const domain = url.hostname.replace('www.', '');
                referrerCounts[domain] = (referrerCounts[domain] || 0) + 1;
            } catch {
                referrerCounts['direct'] = (referrerCounts['direct'] || 0) + 1;
            }
        });
        
        // 直接流入の数も追加
        const directCount = db.prepare(`
            SELECT COUNT(*) as count FROM page_views 
            WHERE DATE(created_at) BETWEEN DATE(?) AND DATE(?)
            AND (referrer IS NULL OR referrer = '')
        `).get(start, end);
        
        referrerCounts['direct'] = (referrerCounts['direct'] || 0) + directCount.count;
        
        // 上位10件を返す
        const sorted = Object.entries(referrerCounts)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 10)
            .map(([domain, count]) => ({ domain, count }));
        
        res.json({ referrers: sorted });
    } catch (error) {
        console.error('Error getting referrer analytics:', error);
        res.status(500).json({ error: 'Failed to get referrer analytics' });
    }
});

// ページ別アクセス分析
app.get('/api/admin/analytics/pages', adminAuth, (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = startDate || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
        const end = endDate || new Date().toISOString().split('T')[0];
        
        const pageStats = db.prepare(`
            SELECT page, COUNT(*) as count 
            FROM page_views 
            WHERE DATE(created_at) BETWEEN DATE(?) AND DATE(?)
            GROUP BY page 
            ORDER BY count DESC
        `).all(start, end);
        
        res.json({ pages: pageStats });
    } catch (error) {
        console.error('Error getting page analytics:', error);
        res.status(500).json({ error: 'Failed to get page analytics' });
    }
});

// コンバージョン分析
app.get('/api/admin/analytics/conversion', adminAuth, (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = startDate || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
        const end = endDate || new Date().toISOString().split('T')[0];
        
        // ホームページのアクセス
        const homeViews = db.prepare(`
            SELECT COUNT(*) as count FROM page_views 
            WHERE page = 'home' AND DATE(created_at) BETWEEN DATE(?) AND DATE(?)
        `).get(start, end);
        
        // クイズ開始
        const quizViews = db.prepare(`
            SELECT COUNT(*) as count FROM page_views 
            WHERE page = 'quiz' AND DATE(created_at) BETWEEN DATE(?) AND DATE(?)
        `).get(start, end);
        
        // 結果ページ
        const resultViews = db.prepare(`
            SELECT COUNT(*) as count FROM page_views 
            WHERE page = 'result' AND DATE(created_at) BETWEEN DATE(?) AND DATE(?)
        `).get(start, end);
        
        // 診断完了数
        const completed = db.prepare(`
            SELECT COUNT(*) as count FROM diagnosis_results 
            WHERE DATE(created_at) BETWEEN DATE(?) AND DATE(?)
        `).get(start, end);
        
        const funnel = [
            { stage: 'ホーム', count: homeViews.count },
            { stage: 'クイズ開始', count: quizViews.count },
            { stage: '結果表示', count: resultViews.count },
            { stage: '診断完了', count: completed.count }
        ];
        
        // コンバージョン率計算
        const conversionRate = homeViews.count > 0 
            ? Math.round(completed.count / homeViews.count * 100 * 10) / 10 
            : 0;
        
        const quizStartRate = homeViews.count > 0
            ? Math.round(quizViews.count / homeViews.count * 100 * 10) / 10
            : 0;
            
        const quizCompleteRate = quizViews.count > 0
            ? Math.round(completed.count / quizViews.count * 100 * 10) / 10
            : 0;
        
        res.json({
            funnel,
            conversionRate,
            quizStartRate,
            quizCompleteRate
        });
    } catch (error) {
        console.error('Error getting conversion analytics:', error);
        res.status(500).json({ error: 'Failed to get conversion analytics' });
    }
});

// タイプ別トレンド分析
app.get('/api/admin/analytics/type-trend', adminAuth, (req, res) => {
    try {
        const { startDate, endDate, typeCode } = req.query;
        const start = startDate || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
        const end = endDate || new Date().toISOString().split('T')[0];
        
        let query = `
            SELECT DATE(created_at) as date, type_code, COUNT(*) as count 
            FROM diagnosis_results 
            WHERE DATE(created_at) BETWEEN DATE(?) AND DATE(?)
        `;
        const params = [start, end];
        
        if (typeCode) {
            query += ` AND type_code = ?`;
            params.push(typeCode);
        }
        
        query += ` GROUP BY date, type_code ORDER BY date, type_code`;
        
        const trends = db.prepare(query).all(...params);
        
        res.json({ trends });
    } catch (error) {
        console.error('Error getting type trend:', error);
        res.status(500).json({ error: 'Failed to get type trend' });
    }
});

// ヒートマップ用データ（曜日 x 時間帯）
app.get('/api/admin/analytics/heatmap', adminAuth, (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = startDate || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
        const end = endDate || new Date().toISOString().split('T')[0];
        
        const heatmapData = db.prepare(`
            SELECT 
                strftime('%w', created_at) as weekday,
                strftime('%H', created_at) as hour,
                COUNT(*) as count 
            FROM page_views 
            WHERE DATE(created_at) BETWEEN DATE(?) AND DATE(?)
            GROUP BY weekday, hour
            ORDER BY weekday, hour
        `).all(start, end);
        
        // 7x24のマトリックスを作成
        const matrix = Array(7).fill(null).map(() => Array(24).fill(0));
        
        heatmapData.forEach(row => {
            const weekday = parseInt(row.weekday);
            const hour = parseInt(row.hour);
            matrix[weekday][hour] = row.count;
        });
        
        res.json({ matrix, maxValue: Math.max(...heatmapData.map(d => d.count), 1) });
    } catch (error) {
        console.error('Error getting heatmap data:', error);
        res.status(500).json({ error: 'Failed to get heatmap data' });
    }
});

// CSVエクスポート - 診断結果
app.get('/api/admin/export/diagnosis', adminAuth, (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = startDate || '2020-01-01';
        const end = endDate || new Date().toISOString().split('T')[0];
        
        const results = db.prepare(`
            SELECT id, type_code, type_name, scores, user_agent, created_at 
            FROM diagnosis_results 
            WHERE DATE(created_at) BETWEEN DATE(?) AND DATE(?)
            ORDER BY created_at DESC
        `).all(start, end);
        
        // CSV生成
        const headers = ['ID', 'タイプコード', 'タイプ名', 'スコア', 'ユーザーエージェント', '日時'];
        const csv = [
            headers.join(','),
            ...results.map(r => [
                r.id,
                r.type_code,
                `"${r.type_name}"`,
                `"${r.scores || ''}"`,
                `"${(r.user_agent || '').replace(/"/g, '""')}"`,
                r.created_at
            ].join(','))
        ].join('\n');
        
        res.setHeader('Content-Type', 'text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', `attachment; filename=diagnosis_${start}_${end}.csv`);
        res.send('\uFEFF' + csv); // BOM付きUTF-8
    } catch (error) {
        console.error('Error exporting diagnosis:', error);
        res.status(500).json({ error: 'Failed to export diagnosis' });
    }
});

// CSVエクスポート - ページビュー
app.get('/api/admin/export/pageviews', adminAuth, (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = startDate || '2020-01-01';
        const end = endDate || new Date().toISOString().split('T')[0];
        
        const results = db.prepare(`
            SELECT id, page, user_agent, referrer, ip, created_at 
            FROM page_views 
            WHERE DATE(created_at) BETWEEN DATE(?) AND DATE(?)
            ORDER BY created_at DESC
        `).all(start, end);
        
        // CSV生成
        const headers = ['ID', 'ページ', 'ユーザーエージェント', 'リファラー', 'IP', '日時'];
        const csv = [
            headers.join(','),
            ...results.map(r => [
                r.id,
                r.page,
                `"${(r.user_agent || '').replace(/"/g, '""')}"`,
                `"${(r.referrer || '').replace(/"/g, '""')}"`,
                `"${r.ip || ''}"`,
                r.created_at
            ].join(','))
        ].join('\n');
        
        res.setHeader('Content-Type', 'text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', `attachment; filename=pageviews_${start}_${end}.csv`);
        res.send('\uFEFF' + csv); // BOM付きUTF-8
    } catch (error) {
        console.error('Error exporting pageviews:', error);
        res.status(500).json({ error: 'Failed to export pageviews' });
    }
});

// サーバー起動
app.listen(PORT, () => {
    console.log(`🚀 マリッジタイプ診断 管理サーバー起動`);
    console.log(`   URL: http://localhost:${PORT}`);
    console.log(`   管理画面: http://localhost:${PORT}/admin`);
    console.log(`   APIキー: ${ADMIN_API_KEY}`);
});
