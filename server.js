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

// 管理画面の静的ファイル配信
app.use('/admin', express.static(path.join(__dirname, '../admin-panel')));

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

// サーバー起動
app.listen(PORT, () => {
    console.log(`🚀 マリッジタイプ診断 管理サーバー起動`);
    console.log(`   URL: http://localhost:${PORT}`);
    console.log(`   管理画面: http://localhost:${PORT}/admin`);
    console.log(`   APIキー: ${ADMIN_API_KEY}`);
});
