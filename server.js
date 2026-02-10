/**
 * マリッジタイプ診断 - 管理用バックエンドサーバー
 * Supabase版（データ永続化対応）
 */

const express = require('express');
const cors = require('cors');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');

const app = express();
const PORT = process.env.PORT || 3001;

// Supabase設定
const SUPABASE_URL = process.env.SUPABASE_URL || 'https://xflhnfrtbkjvopaueitb.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_KEY || 'sb_secret_oFVC9jo-NVJy8zQT087UgQ_Q8pHDsAx';
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// ミドルウェア
app.use(cors());
app.use(express.json());

// 日本時間（JST, UTC+9）ヘルパー関数
function getJSTDate() {
    const now = new Date();
    // UTC時間に9時間足して日本時間に
    return new Date(now.getTime() + (9 * 60 * 60 * 1000));
}

function getJSTDateString(date = new Date()) {
    const jstDate = new Date(date.getTime() + (9 * 60 * 60 * 1000));
    const year = jstDate.getUTCFullYear();
    const month = String(jstDate.getUTCMonth() + 1).padStart(2, '0');
    const day = String(jstDate.getUTCDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
}

function getJSTHour(dateStr) {
    const date = new Date(dateStr);
    // UTC時間に9時間足して日本時間に変換
    const jstDate = new Date(date.getTime() + (9 * 60 * 60 * 1000));
    return jstDate.getUTCHours();
}

function getJSTWeekday(dateStr) {
    const date = new Date(dateStr);
    // UTC時間に9時間足して日本時間に変換
    const jstDate = new Date(date.getTime() + (9 * 60 * 60 * 1000));
    return jstDate.getUTCDay();
}

// 静的ファイル配信
app.use('/admin', express.static(path.join(__dirname, 'admin-panel')));

// ヘルスチェック（Render用）
app.get('/api/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString(), database: 'supabase' });
});

// デバッグ用API
app.get('/api/debug/daily', async (req, res) => {
    try {
        const sevenDaysAgoDate = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
        const sevenDaysAgo = getJSTDateString(sevenDaysAgoDate) + 'T00:00:00+09:00';
        const today = getJSTDateString();
        
        const { data: viewsData, error } = await supabase
            .from('page_views')
            .select('created_at')
            .gte('created_at', sevenDaysAgo)
            .order('created_at', { ascending: false })
            .limit(10000);
        
        const dailyViews = {};
        (viewsData || []).forEach(v => {
            const date = getJSTDateString(new Date(v.created_at));
            dailyViews[date] = (dailyViews[date] || 0) + 1;
        });
        
        res.json({
            serverTime: new Date().toISOString(),
            todayJST: today,
            sevenDaysAgo,
            sevenDaysAgoDate: sevenDaysAgoDate.toISOString(),
            viewsDataCount: (viewsData || []).length,
            sampleData: (viewsData || []).slice(-5),
            dailyViews,
            error: error ? error.message : null
        });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ==========================================
// API: データ記録
// ==========================================

// ページビュー記録（UTMパラメータ対応）
app.post('/api/track/pageview', async (req, res) => {
    try {
        const { page, utm_source, utm_medium, utm_campaign } = req.body;
        const userAgent = req.headers['user-agent'] || '';
        const referrer = req.headers['referer'] || '';
        const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress || '';
        
        const { error } = await supabase
            .from('page_views')
            .insert({ 
                page, 
                user_agent: userAgent, 
                referrer, 
                ip,
                utm_source: utm_source || null,
                utm_medium: utm_medium || null,
                utm_campaign: utm_campaign || null
            });
        
        if (error) throw error;
        res.json({ success: true });
    } catch (error) {
        console.error('Error tracking pageview:', error);
        res.status(500).json({ error: 'Failed to track pageview' });
    }
});

// クイズ進捗記録（離脱ポイント分析用）
app.post('/api/track/quiz-progress', async (req, res) => {
    try {
        const { session_id, question_number, action } = req.body;
        const userAgent = req.headers['user-agent'] || '';
        
        // UTMパラメータも一緒に記録
        const { utm_source, utm_medium, utm_campaign } = req.body;
        
        const { error } = await supabase
            .from('quiz_progress')
            .insert({ 
                session_id,
                question_number,
                action, // 'start', 'answer', 'complete'
                user_agent: userAgent,
                utm_source: utm_source || null,
                utm_medium: utm_medium || null,
                utm_campaign: utm_campaign || null
            });
        
        if (error) throw error;
        res.json({ success: true });
    } catch (error) {
        console.error('Error tracking quiz progress:', error);
        res.status(500).json({ error: 'Failed to track quiz progress' });
    }
});

// 診断結果記録
app.post('/api/track/diagnosis', async (req, res) => {
    try {
        const { typeCode, typeName, scores } = req.body;
        const userAgent = req.headers['user-agent'] || '';
        
        const { error } = await supabase
            .from('diagnosis_results')
            .insert({ 
                type_code: typeCode, 
                type_name: typeName, 
                scores: JSON.stringify(scores), 
                user_agent: userAgent 
            });
        
        if (error) throw error;
        res.json({ success: true });
    } catch (error) {
        console.error('Error tracking diagnosis:', error);
        res.status(500).json({ error: 'Failed to track diagnosis' });
    }
});

// 恋愛占い実行記録（管理分析用）
app.post('/api/track/love-fortune', async (req, res) => {
    try {
        const { typeCode, gender, hasLover } = req.body;
        const { error } = await supabase
            .from('feature_events')
            .insert({
                event_type: 'love_fortune',
                payload: { type_code: typeCode || null, gender: gender || null, has_lover: hasLover || null }
            });
        if (error) throw error;
        res.json({ success: true });
    } catch (error) {
        console.error('Error tracking love-fortune:', error);
        res.status(500).json({ error: 'Failed to track love-fortune' });
    }
});

// 相性診断実行記録（管理分析用）
app.post('/api/track/compatibility', async (req, res) => {
    try {
        const { myType, partnerType } = req.body;
        const { error } = await supabase
            .from('feature_events')
            .insert({
                event_type: 'compatibility',
                payload: { my_type: myType || null, partner_type: partnerType || null }
            });
        if (error) throw error;
        res.json({ success: true });
    } catch (error) {
        console.error('Error tracking compatibility:', error);
        res.status(500).json({ error: 'Failed to track compatibility' });
    }
});

// 診断コード発行記録（管理分析用）
app.post('/api/track/diagnosis-code', async (req, res) => {
    try {
        const { code, type } = req.body;
        if (!code || !type) {
            return res.status(400).json({ error: 'code and type required' });
        }
        const { error } = await supabase
            .from('feature_events')
            .insert({
                event_type: 'diagnosis_code',
                payload: { code: String(code).toUpperCase(), type: String(type).toUpperCase() }
            });
        if (error) throw error;
        res.json({ success: true });
    } catch (error) {
        console.error('Error tracking diagnosis-code:', error);
        res.status(500).json({ error: 'Failed to track diagnosis-code' });
    }
});

// ==========================================
// API: 統計データ取得（管理画面用）
// ==========================================

// 管理者認証ミドルウェア
const ADMIN_API_KEY = process.env.ADMIN_API_KEY || 'marriage-type-admin-2026';

const adminAuth = (req, res, next) => {
    const apiKey = req.headers['x-api-key'];
    if (apiKey !== ADMIN_API_KEY) {
        return res.status(401).json({ error: 'Unauthorized' });
    }
    next();
};

// ダッシュボード概要
app.get('/api/admin/dashboard', adminAuth, async (req, res) => {
    try {
        // 日本時間で「今日」を取得
        const today = getJSTDateString();
        // 日本時間の00:00:00はUTCの前日15:00:00
        const todayStart = today + 'T00:00:00+09:00';
        const todayEnd = today + 'T23:59:59+09:00';
        
        // 総アクセス数（PV）
        const { count: totalViews } = await supabase
            .from('page_views')
            .select('*', { count: 'exact', head: true });
        
        // 総UU数（ユニークIP）
        const { data: allIpData } = await supabase
            .from('page_views')
            .select('ip');
        const totalUU = new Set((allIpData || []).map(d => d.ip).filter(ip => ip)).size;
        
        // 今日のアクセス数（PV）
        const { count: todayViews } = await supabase
            .from('page_views')
            .select('*', { count: 'exact', head: true })
            .gte('created_at', todayStart)
            .lte('created_at', todayEnd);
        
        // 今日のUU数
        const { data: todayIpData } = await supabase
            .from('page_views')
            .select('ip')
            .gte('created_at', todayStart)
            .lte('created_at', todayEnd);
        const todayUU = new Set((todayIpData || []).map(d => d.ip).filter(ip => ip)).size;
        
        // 総診断数
        const { count: totalDiagnosis } = await supabase
            .from('diagnosis_results')
            .select('*', { count: 'exact', head: true });
        
        // 今日の診断数
        const { count: todayDiagnosis } = await supabase
            .from('diagnosis_results')
            .select('*', { count: 'exact', head: true })
            .gte('created_at', todayStart)
            .lte('created_at', todayEnd);
        
        // タイプ別診断数
        const { data: diagnosisData } = await supabase
            .from('diagnosis_results')
            .select('type_code, type_name');
        
        const typeStats = {};
        (diagnosisData || []).forEach(d => {
            if (!typeStats[d.type_code]) {
                typeStats[d.type_code] = { type_code: d.type_code, type_name: d.type_name, count: 0 };
            }
            typeStats[d.type_code].count++;
        });
        const typeStatsArray = Object.values(typeStats).sort((a, b) => b.count - a.count);
        
        // 過去7日間のアクセス推移（日本時間基準）
        const sevenDaysAgoDate = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
        const sevenDaysAgo = getJSTDateString(sevenDaysAgoDate) + 'T00:00:00+09:00';
        const { data: viewsData } = await supabase
            .from('page_views')
            .select('created_at')
            .gte('created_at', sevenDaysAgo)
            .order('created_at', { ascending: false })
            .limit(10000);
        
        const dailyViews = {};
        (viewsData || []).forEach(v => {
            // 日本時間で日付を取得
            const date = getJSTDateString(new Date(v.created_at));
            dailyViews[date] = (dailyViews[date] || 0) + 1;
        });
        const dailyViewsArray = Object.entries(dailyViews)
            .map(([date, count]) => ({ date, count }))
            .sort((a, b) => a.date.localeCompare(b.date));
        
        // 過去7日間の診断数推移（同じ日本時間基準）
        const { data: diagData } = await supabase
            .from('diagnosis_results')
            .select('created_at')
            .gte('created_at', sevenDaysAgo)
            .order('created_at', { ascending: false })
            .limit(10000);
        
        const dailyDiagnosis = {};
        (diagData || []).forEach(d => {
            // 日本時間で日付を取得
            const date = getJSTDateString(new Date(d.created_at));
            dailyDiagnosis[date] = (dailyDiagnosis[date] || 0) + 1;
        });
        const dailyDiagnosisArray = Object.entries(dailyDiagnosis)
            .map(([date, count]) => ({ date, count }))
            .sort((a, b) => a.date.localeCompare(b.date));
        
        res.json({
            totalViews: totalViews || 0,
            totalUU: totalUU || 0,
            todayViews: todayViews || 0,
            todayUU: todayUU || 0,
            totalDiagnosis: totalDiagnosis || 0,
            todayDiagnosis: todayDiagnosis || 0,
            typeStats: typeStatsArray,
            dailyViews: dailyViewsArray,
            dailyDiagnosis: dailyDiagnosisArray
        });
    } catch (error) {
        console.error('Error getting dashboard data:', error);
        res.status(500).json({ error: 'Failed to get dashboard data' });
    }
});

// 最新の診断結果一覧
app.get('/api/admin/diagnosis/recent', adminAuth, async (req, res) => {
    try {
        const limit = parseInt(req.query.limit) || 50;
        
        const { data, error } = await supabase
            .from('diagnosis_results')
            .select('id, type_code, type_name, created_at')
            .order('created_at', { ascending: false })
            .limit(limit);
        
        if (error) throw error;
        res.json(data || []);
    } catch (error) {
        console.error('Error getting recent diagnosis:', error);
        res.status(500).json({ error: 'Failed to get recent diagnosis' });
    }
});

// 機能別イベント数（恋愛占い・相性診断・診断コード発行）
app.get('/api/admin/analytics/features', adminAuth, async (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = (startDate || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0]) + 'T00:00:00.000Z';
        const end = (endDate || new Date().toISOString().split('T')[0]) + 'T23:59:59.999Z';

        const todayJST = getJSTDateString();
        const todayStartJST = new Date(todayJST + 'T00:00:00+09:00');
        const todayEndJST = new Date(todayJST + 'T23:59:59.999+09:00');
        const todayStart = todayStartJST.toISOString();
        const todayEnd = todayEndJST.toISOString();

        const { data: allRows } = await supabase
            .from('feature_events')
            .select('id, event_type, payload, created_at')
            .order('created_at', { ascending: false })
            .limit(100000);

        const inRange = (rows, startStr, endStr) => {
            return (rows || []).filter(r => {
                const t = (r.created_at && r.created_at.endsWith('Z') ? r.created_at : new Date(r.created_at).toISOString());
                return t >= startStr && t <= endStr;
            });
        };

        const periodRows = inRange(allRows, start, end);
        const todayRows = inRange(allRows, todayStart, todayEnd);

        const countByType = (rows, type) => rows.filter(r => r.event_type === type).length;

        res.json({
            period: { start: startDate, end: endDate },
            loveFortune: {
                total: (allRows || []).filter(r => r.event_type === 'love_fortune').length,
                period: countByType(periodRows, 'love_fortune'),
                today: countByType(todayRows, 'love_fortune')
            },
            compatibility: {
                total: (allRows || []).filter(r => r.event_type === 'compatibility').length,
                period: countByType(periodRows, 'compatibility'),
                today: countByType(todayRows, 'compatibility')
            },
            diagnosisCode: {
                total: (allRows || []).filter(r => r.event_type === 'diagnosis_code').length,
                period: countByType(periodRows, 'diagnosis_code'),
                today: countByType(todayRows, 'diagnosis_code')
            }
        });
    } catch (error) {
        console.error('Error getting feature analytics:', error);
        res.status(500).json({ error: 'Failed to get feature analytics' });
    }
});

// 発行された診断コード一覧（管理画面用）
app.get('/api/admin/diagnosis-codes', adminAuth, async (req, res) => {
    try {
        const { startDate, endDate, limit } = req.query;
        const limitNum = Math.min(parseInt(limit) || 100, 500);
        const start = (startDate || '2020-01-01') + 'T00:00:00';
        const end = (endDate || new Date().toISOString().split('T')[0]) + 'T23:59:59';

        const { data, error } = await supabase
            .from('feature_events')
            .select('id, event_type, payload, created_at')
            .eq('event_type', 'diagnosis_code')
            .gte('created_at', start)
            .lte('created_at', end)
            .order('created_at', { ascending: false })
            .limit(limitNum);

        if (error) throw error;

        const list = (data || []).map(r => ({
            id: r.id,
            code: (r.payload && r.payload.code) || '',
            type: (r.payload && r.payload.type) || '',
            created_at: r.created_at
        }));

        res.json({ list, total: list.length });
    } catch (error) {
        console.error('Error getting diagnosis codes:', error);
        res.status(500).json({ error: 'Failed to get diagnosis codes' });
    }
});

// ==========================================
// API: 高度な分析機能
// ==========================================

// 期間指定ダッシュボード
app.get('/api/admin/analytics', adminAuth, async (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = (startDate || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0]) + 'T00:00:00';
        const end = (endDate || new Date().toISOString().split('T')[0]) + 'T23:59:59';
        
        // 期間内のアクセス数（PV）
        const { count: periodViews } = await supabase
            .from('page_views')
            .select('*', { count: 'exact', head: true })
            .gte('created_at', start)
            .lte('created_at', end);
        
        // 期間内のUU数
        const { data: periodIpData } = await supabase
            .from('page_views')
            .select('ip')
            .gte('created_at', start)
            .lte('created_at', end);
        const periodUU = new Set((periodIpData || []).map(d => d.ip).filter(ip => ip)).size;
        
        // 期間内の診断数
        const { count: periodDiagnosis } = await supabase
            .from('diagnosis_results')
            .select('*', { count: 'exact', head: true })
            .gte('created_at', start)
            .lte('created_at', end);
        
        // 日別アクセス推移（PVとUU両方）
        const { data: viewsData } = await supabase
            .from('page_views')
            .select('created_at, ip')
            .gte('created_at', start)
            .lte('created_at', end)
            .order('created_at', { ascending: false })
            .limit(50000);
        
        const dailyViews = {};
        const dailyUU = {};  // 日別UU用
        (viewsData || []).forEach(v => {
            // 日本時間で日付を取得
            const date = getJSTDateString(new Date(v.created_at));
            dailyViews[date] = (dailyViews[date] || 0) + 1;
            
            // 日別UU集計
            if (!dailyUU[date]) dailyUU[date] = new Set();
            if (v.ip) dailyUU[date].add(v.ip);
        });
        const dailyViewsArray = Object.entries(dailyViews)
            .map(([date, count]) => ({ date, count }))
            .sort((a, b) => a.date.localeCompare(b.date));
        const dailyUUArray = Object.entries(dailyUU)
            .map(([date, ipSet]) => ({ date, count: ipSet.size }))
            .sort((a, b) => a.date.localeCompare(b.date));
        
        // 日別診断推移
        const { data: diagData } = await supabase
            .from('diagnosis_results')
            .select('created_at, type_code, type_name')
            .gte('created_at', start)
            .lte('created_at', end)
            .order('created_at', { ascending: false })
            .limit(50000);
        
        const dailyDiagnosis = {};
        const typeStats = {};
        (diagData || []).forEach(d => {
            // 日本時間で日付を取得
            const date = getJSTDateString(new Date(d.created_at));
            dailyDiagnosis[date] = (dailyDiagnosis[date] || 0) + 1;
            
            if (!typeStats[d.type_code]) {
                typeStats[d.type_code] = { type_code: d.type_code, type_name: d.type_name, count: 0 };
            }
            typeStats[d.type_code].count++;
        });
        const dailyDiagnosisArray = Object.entries(dailyDiagnosis)
            .map(([date, count]) => ({ date, count }))
            .sort((a, b) => a.date.localeCompare(b.date));
        const typeStatsArray = Object.values(typeStats).sort((a, b) => b.count - a.count);
        
        res.json({
            period: { start: startDate, end: endDate },
            periodViews: periodViews || 0,
            periodUU: periodUU || 0,
            periodDiagnosis: periodDiagnosis || 0,
            dailyViews: dailyViewsArray,
            dailyUU: dailyUUArray,
            dailyDiagnosis: dailyDiagnosisArray,
            typeStats: typeStatsArray
        });
    } catch (error) {
        console.error('Error getting analytics:', error);
        res.status(500).json({ error: 'Failed to get analytics' });
    }
});

// 時間帯別分析
app.get('/api/admin/analytics/hourly', adminAuth, async (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = (startDate || new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0]) + 'T00:00:00';
        const end = (endDate || new Date().toISOString().split('T')[0]) + 'T23:59:59';
        
        const { data: viewsData } = await supabase
            .from('page_views')
            .select('created_at')
            .gte('created_at', start)
            .lte('created_at', end);
        
        const { data: diagData } = await supabase
            .from('diagnosis_results')
            .select('created_at')
            .gte('created_at', start)
            .lte('created_at', end);
        
        const hourlyViews = {};
        const hourlyDiagnosis = {};
        
        // 日本時間（JST）で時間帯をカウント
        (viewsData || []).forEach(v => {
            const hour = getJSTHour(v.created_at).toString().padStart(2, '0');
            hourlyViews[hour] = (hourlyViews[hour] || 0) + 1;
        });
        
        (diagData || []).forEach(d => {
            const hour = getJSTHour(d.created_at).toString().padStart(2, '0');
            hourlyDiagnosis[hour] = (hourlyDiagnosis[hour] || 0) + 1;
        });
        
        res.json({
            hourlyViews: Object.entries(hourlyViews).map(([hour, count]) => ({ hour, count })),
            hourlyDiagnosis: Object.entries(hourlyDiagnosis).map(([hour, count]) => ({ hour, count }))
        });
    } catch (error) {
        console.error('Error getting hourly analytics:', error);
        res.status(500).json({ error: 'Failed to get hourly analytics' });
    }
});

// 曜日別分析
app.get('/api/admin/analytics/weekday', adminAuth, async (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = (startDate || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0]) + 'T00:00:00';
        const end = (endDate || new Date().toISOString().split('T')[0]) + 'T23:59:59';
        
        const { data: viewsData } = await supabase
            .from('page_views')
            .select('created_at')
            .gte('created_at', start)
            .lte('created_at', end);
        
        const { data: diagData } = await supabase
            .from('diagnosis_results')
            .select('created_at')
            .gte('created_at', start)
            .lte('created_at', end);
        
        const weekdayViews = {};
        const weekdayDiagnosis = {};
        
        // 日本時間（JST）で曜日をカウント
        (viewsData || []).forEach(v => {
            const weekday = getJSTWeekday(v.created_at).toString();
            weekdayViews[weekday] = (weekdayViews[weekday] || 0) + 1;
        });
        
        (diagData || []).forEach(d => {
            const weekday = getJSTWeekday(d.created_at).toString();
            weekdayDiagnosis[weekday] = (weekdayDiagnosis[weekday] || 0) + 1;
        });
        
        res.json({
            weekdayViews: Object.entries(weekdayViews).map(([weekday, count]) => ({ weekday, count })),
            weekdayDiagnosis: Object.entries(weekdayDiagnosis).map(([weekday, count]) => ({ weekday, count }))
        });
    } catch (error) {
        console.error('Error getting weekday analytics:', error);
        res.status(500).json({ error: 'Failed to get weekday analytics' });
    }
});

// デバイス分析
app.get('/api/admin/analytics/devices', adminAuth, async (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = (startDate || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0]) + 'T00:00:00';
        const end = (endDate || new Date().toISOString().split('T')[0]) + 'T23:59:59';
        
        const { data } = await supabase
            .from('page_views')
            .select('user_agent')
            .gte('created_at', start)
            .lte('created_at', end);
        
        const devices = { mobile: 0, tablet: 0, desktop: 0 };
        
        (data || []).forEach(row => {
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
app.get('/api/admin/analytics/referrers', adminAuth, async (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = (startDate || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0]) + 'T00:00:00';
        const end = (endDate || new Date().toISOString().split('T')[0]) + 'T23:59:59';
        
        const { data } = await supabase
            .from('page_views')
            .select('referrer')
            .gte('created_at', start)
            .lte('created_at', end);
        
        const referrerCounts = { 'direct': 0 };
        
        (data || []).forEach(row => {
            if (!row.referrer || row.referrer === '') {
                referrerCounts['direct']++;
            } else {
                try {
                    const url = new URL(row.referrer);
                    const domain = url.hostname.replace('www.', '');
                    referrerCounts[domain] = (referrerCounts[domain] || 0) + 1;
                } catch {
                    referrerCounts['direct']++;
                }
            }
        });
        
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
app.get('/api/admin/analytics/pages', adminAuth, async (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = (startDate || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0]) + 'T00:00:00';
        const end = (endDate || new Date().toISOString().split('T')[0]) + 'T23:59:59';
        
        const { data } = await supabase
            .from('page_views')
            .select('page')
            .gte('created_at', start)
            .lte('created_at', end);
        
        const pageCounts = {};
        (data || []).forEach(row => {
            pageCounts[row.page] = (pageCounts[row.page] || 0) + 1;
        });
        
        const pages = Object.entries(pageCounts)
            .map(([page, count]) => ({ page, count }))
            .sort((a, b) => b.count - a.count);
        
        res.json({ pages });
    } catch (error) {
        console.error('Error getting page analytics:', error);
        res.status(500).json({ error: 'Failed to get page analytics' });
    }
});

// コンバージョン分析
app.get('/api/admin/analytics/conversion', adminAuth, async (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = (startDate || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0]) + 'T00:00:00';
        const end = (endDate || new Date().toISOString().split('T')[0]) + 'T23:59:59';
        
        const { count: homeViews } = await supabase
            .from('page_views')
            .select('*', { count: 'exact', head: true })
            .eq('page', 'home')
            .gte('created_at', start)
            .lte('created_at', end);
        
        const { count: quizViews } = await supabase
            .from('page_views')
            .select('*', { count: 'exact', head: true })
            .eq('page', 'quiz')
            .gte('created_at', start)
            .lte('created_at', end);
        
        const { count: resultViews } = await supabase
            .from('page_views')
            .select('*', { count: 'exact', head: true })
            .eq('page', 'result')
            .gte('created_at', start)
            .lte('created_at', end);
        
        const { count: completed } = await supabase
            .from('diagnosis_results')
            .select('*', { count: 'exact', head: true })
            .gte('created_at', start)
            .lte('created_at', end);
        
        const funnel = [
            { stage: 'ホーム', count: homeViews || 0 },
            { stage: 'クイズ開始', count: quizViews || 0 },
            { stage: '結果表示', count: resultViews || 0 },
            { stage: '診断完了', count: completed || 0 }
        ];
        
        const conversionRate = (homeViews || 0) > 0 
            ? Math.round((completed || 0) / homeViews * 100 * 10) / 10 
            : 0;
        
        const quizStartRate = (homeViews || 0) > 0
            ? Math.round((quizViews || 0) / homeViews * 100 * 10) / 10
            : 0;
            
        const quizCompleteRate = (quizViews || 0) > 0
            ? Math.round((completed || 0) / quizViews * 100 * 10) / 10
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

// ヒートマップ用データ
app.get('/api/admin/analytics/heatmap', adminAuth, async (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = (startDate || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0]) + 'T00:00:00';
        const end = (endDate || new Date().toISOString().split('T')[0]) + 'T23:59:59';
        
        const { data } = await supabase
            .from('page_views')
            .select('created_at')
            .gte('created_at', start)
            .lte('created_at', end);
        
        const matrix = Array(7).fill(null).map(() => Array(24).fill(0));
        let maxValue = 1;
        
        // 日本時間（JST）で曜日・時間帯をカウント
        (data || []).forEach(row => {
            const weekday = getJSTWeekday(row.created_at);
            const hour = getJSTHour(row.created_at);
            matrix[weekday][hour]++;
            if (matrix[weekday][hour] > maxValue) {
                maxValue = matrix[weekday][hour];
            }
        });
        
        res.json({ matrix, maxValue });
    } catch (error) {
        console.error('Error getting heatmap data:', error);
        res.status(500).json({ error: 'Failed to get heatmap data' });
    }
});

// ==========================================
// API: 離脱ポイント分析
// ==========================================

// 離脱ファネル分析
app.get('/api/admin/analytics/dropout', adminAuth, async (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = (startDate || getJSTDateString(new Date(Date.now() - 30 * 24 * 60 * 60 * 1000))) + 'T00:00:00+09:00';
        const end = (endDate || getJSTDateString()) + 'T23:59:59+09:00';
        
        const { data } = await supabase
            .from('quiz_progress')
            .select('session_id, question_number, action')
            .gte('created_at', start)
            .lte('created_at', end);
        
        // セッションごとに最大到達設問を集計
        const sessions = {};
        (data || []).forEach(row => {
            if (!sessions[row.session_id]) {
                sessions[row.session_id] = { maxQuestion: 0, completed: false };
            }
            if (row.question_number > sessions[row.session_id].maxQuestion) {
                sessions[row.session_id].maxQuestion = row.question_number;
            }
            if (row.action === 'complete') {
                sessions[row.session_id].completed = true;
            }
        });
        
        // ページ単位での到達数を集計（5問ごと = 4ページ）
        // Q1=ページ1開始, Q6=ページ2開始, Q11=ページ3開始, Q16=ページ4開始, Q20=完了
        const pageMilestones = [1, 6, 11, 16, 20];
        const pageLabels = ['ページ1 (Q1-Q5)', 'ページ2 (Q6-Q10)', 'ページ3 (Q11-Q15)', 'ページ4 (Q16-Q20)', '診断完了'];
        const pageCounts = {};
        let completedCount = 0;
        const totalSessions = Object.keys(sessions).length;
        
        Object.values(sessions).forEach(s => {
            // 各マイルストーンに到達したかチェック
            pageMilestones.forEach(milestone => {
                if (s.maxQuestion >= milestone) {
                    pageCounts[milestone] = (pageCounts[milestone] || 0) + 1;
                }
            });
            if (s.completed) completedCount++;
        });
        
        // ファネルデータ作成（ページ単位）
        const funnel = [];
        pageMilestones.forEach((milestone, idx) => {
            const count = pageCounts[milestone] || 0;
            const prevCount = idx === 0 ? totalSessions : (pageCounts[pageMilestones[idx - 1]] || 0);
            const dropoutRate = prevCount > 0 ? Math.round((1 - count / prevCount) * 100 * 10) / 10 : 0;
            funnel.push({
                question: milestone,
                label: pageLabels[idx],
                reached: count,
                dropoutRate: dropoutRate
            });
        });
        
        // 完了率
        const completionRate = totalSessions > 0 ? Math.round(completedCount / totalSessions * 100 * 10) / 10 : 0;
        
        res.json({
            totalSessions,
            completedCount,
            completionRate,
            funnel
        });
    } catch (error) {
        console.error('Error getting dropout analytics:', error);
        res.status(500).json({ error: 'Failed to get dropout analytics' });
    }
});

// ==========================================
// API: 流入元詳細分析
// ==========================================

// 流入元の詳細分析（リファラー自動判定）
app.get('/api/admin/analytics/traffic-sources', adminAuth, async (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = (startDate || getJSTDateString(new Date(Date.now() - 30 * 24 * 60 * 60 * 1000))) + 'T00:00:00+09:00';
        const end = (endDate || getJSTDateString()) + 'T23:59:59+09:00';
        
        const { data } = await supabase
            .from('page_views')
            .select('referrer, utm_source, utm_medium, utm_campaign, user_agent')
            .gte('created_at', start)
            .lte('created_at', end);
        
        // 流入元を分類
        const sources = {
            organic_search: { count: 0, label: '🔍 検索流入', details: {} },
            social: { count: 0, label: '📱 SNS', details: {} },
            utm_tracked: { count: 0, label: '🔗 トラッキングリンク', details: {} },
            referral: { count: 0, label: '🌐 外部サイト', details: {} },
            direct: { count: 0, label: '📌 直接アクセス', details: {} }
        };
        
        // デバイス分類
        const devices = { ios: 0, android: 0, pc_mac: 0, pc_windows: 0, other: 0 };
        
        // 検索エンジン判定
        const searchEngines = ['google', 'yahoo', 'bing', 'duckduckgo', 'baidu'];
        // SNS判定
        const socialSites = {
            'twitter.com': 'Twitter/X',
            't.co': 'Twitter/X',
            'x.com': 'Twitter/X',
            'instagram.com': 'Instagram',
            'l.instagram.com': 'Instagram',
            'facebook.com': 'Facebook',
            'l.facebook.com': 'Facebook',
            'line.me': 'LINE',
            'lin.ee': 'LINE',
            'tiktok.com': 'TikTok',
            'youtube.com': 'YouTube',
            'youtu.be': 'YouTube'
        };
        
        (data || []).forEach(row => {
            // デバイス判定
            const ua = (row.user_agent || '').toLowerCase();
            if (/iphone|ipad|ipod/.test(ua)) {
                devices.ios++;
            } else if (/android/.test(ua)) {
                devices.android++;
            } else if (/macintosh|mac os/.test(ua)) {
                devices.pc_mac++;
            } else if (/windows/.test(ua)) {
                devices.pc_windows++;
            } else {
                devices.other++;
            }
            
            // UTMトラッキングがある場合
            if (row.utm_source) {
                sources.utm_tracked.count++;
                const key = row.utm_campaign || row.utm_source;
                sources.utm_tracked.details[key] = (sources.utm_tracked.details[key] || 0) + 1;
                return;
            }
            
            // リファラーがない場合は直接アクセス
            if (!row.referrer || row.referrer === '') {
                sources.direct.count++;
                return;
            }
            
            try {
                const refUrl = new URL(row.referrer);
                const domain = refUrl.hostname.toLowerCase().replace('www.', '');
                
                // 検索エンジンかチェック
                const isSearch = searchEngines.some(se => domain.includes(se));
                if (isSearch) {
                    sources.organic_search.count++;
                    sources.organic_search.details[domain] = (sources.organic_search.details[domain] || 0) + 1;
                    return;
                }
                
                // SNSかチェック
                for (const [snsDomain, snsName] of Object.entries(socialSites)) {
                    if (domain.includes(snsDomain)) {
                        sources.social.count++;
                        sources.social.details[snsName] = (sources.social.details[snsName] || 0) + 1;
                        return;
                    }
                }
                
                // その他の外部サイト
                sources.referral.count++;
                sources.referral.details[domain] = (sources.referral.details[domain] || 0) + 1;
                
            } catch {
                sources.direct.count++;
            }
        });
        
        // 詳細をソートして配列に変換
        Object.keys(sources).forEach(key => {
            sources[key].details = Object.entries(sources[key].details)
                .sort((a, b) => b[1] - a[1])
                .slice(0, 10)
                .map(([name, count]) => ({ name, count }));
        });
        
        const total = (data || []).length;
        
        res.json({
            total,
            sources,
            devices: {
                ios: { count: devices.ios, percent: total ? Math.round(devices.ios / total * 100) : 0 },
                android: { count: devices.android, percent: total ? Math.round(devices.android / total * 100) : 0 },
                pc_mac: { count: devices.pc_mac, percent: total ? Math.round(devices.pc_mac / total * 100) : 0 },
                pc_windows: { count: devices.pc_windows, percent: total ? Math.round(devices.pc_windows / total * 100) : 0 },
                other: { count: devices.other, percent: total ? Math.round(devices.other / total * 100) : 0 }
            }
        });
    } catch (error) {
        console.error('Error getting traffic sources:', error);
        res.status(500).json({ error: 'Failed to get traffic sources' });
    }
});

// ==========================================
// API: UTMトラッキング・キャンペーン分析
// ==========================================

// UTMソース別分析
app.get('/api/admin/analytics/utm', adminAuth, async (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = (startDate || getJSTDateString(new Date(Date.now() - 30 * 24 * 60 * 60 * 1000))) + 'T00:00:00+09:00';
        const end = (endDate || getJSTDateString()) + 'T23:59:59+09:00';
        
        const { data } = await supabase
            .from('page_views')
            .select('utm_source, utm_medium, utm_campaign')
            .gte('created_at', start)
            .lte('created_at', end)
            .order('created_at', { ascending: false })
            .limit(50000);
        
        // ソース別集計
        const sourceCounts = {};
        const mediumCounts = {};
        const campaignCounts = {};
        let directCount = 0;
        
        (data || []).forEach(row => {
            if (row.utm_source) {
                sourceCounts[row.utm_source] = (sourceCounts[row.utm_source] || 0) + 1;
            } else {
                directCount++;
            }
            
            if (row.utm_medium) {
                mediumCounts[row.utm_medium] = (mediumCounts[row.utm_medium] || 0) + 1;
            }
            
            if (row.utm_campaign) {
                campaignCounts[row.utm_campaign] = (campaignCounts[row.utm_campaign] || 0) + 1;
            }
        });
        
        const sources = Object.entries(sourceCounts)
            .map(([source, count]) => ({ source, count }))
            .sort((a, b) => b.count - a.count);
        
        const mediums = Object.entries(mediumCounts)
            .map(([medium, count]) => ({ medium, count }))
            .sort((a, b) => b.count - a.count);
        
        const campaigns = Object.entries(campaignCounts)
            .map(([campaign, count]) => ({ campaign, count }))
            .sort((a, b) => b.count - a.count);
        
        res.json({
            sources,
            mediums,
            campaigns,
            directCount,
            totalTracked: (data || []).filter(d => d.utm_source).length
        });
    } catch (error) {
        console.error('Error getting UTM analytics:', error);
        res.status(500).json({ error: 'Failed to get UTM analytics' });
    }
});

// キャンペーン詳細分析
app.get('/api/admin/analytics/campaign/:campaign', adminAuth, async (req, res) => {
    try {
        const { campaign } = req.params;
        const { startDate, endDate } = req.query;
        const start = (startDate || getJSTDateString(new Date(Date.now() - 30 * 24 * 60 * 60 * 1000))) + 'T00:00:00+09:00';
        const end = (endDate || getJSTDateString()) + 'T23:59:59+09:00';
        
        const { data } = await supabase
            .from('page_views')
            .select('page, utm_source, utm_medium, created_at')
            .eq('utm_campaign', campaign)
            .gte('created_at', start)
            .lte('created_at', end);
        
        // 日別集計
        const dailyCounts = {};
        const pageCounts = {};
        
        (data || []).forEach(row => {
            // 日本時間で日付を取得
            const date = new Date(row.created_at);
            const jstDate = new Date(date.getTime() + (9 * 60 * 60 * 1000));
            const dateStr = jstDate.toISOString().split('T')[0];
            dailyCounts[dateStr] = (dailyCounts[dateStr] || 0) + 1;
            pageCounts[row.page] = (pageCounts[row.page] || 0) + 1;
        });
        
        const dailyData = Object.entries(dailyCounts)
            .map(([date, count]) => ({ date, count }))
            .sort((a, b) => a.date.localeCompare(b.date));
        
        const pageData = Object.entries(pageCounts)
            .map(([page, count]) => ({ page, count }))
            .sort((a, b) => b.count - a.count);
        
        res.json({
            campaign,
            totalViews: (data || []).length,
            dailyData,
            pageData
        });
    } catch (error) {
        console.error('Error getting campaign analytics:', error);
        res.status(500).json({ error: 'Failed to get campaign analytics' });
    }
});

// CSVエクスポート - 診断結果
app.get('/api/admin/export/diagnosis', adminAuth, async (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = (startDate || '2020-01-01') + 'T00:00:00';
        const end = (endDate || new Date().toISOString().split('T')[0]) + 'T23:59:59';
        
        const { data, error } = await supabase
            .from('diagnosis_results')
            .select('id, type_code, type_name, scores, user_agent, created_at')
            .gte('created_at', start)
            .lte('created_at', end)
            .order('created_at', { ascending: false });
        
        if (error) throw error;
        
        const headers = ['ID', 'タイプコード', 'タイプ名', 'スコア', 'ユーザーエージェント', '日時'];
        const csv = [
            headers.join(','),
            ...(data || []).map(r => [
                r.id,
                r.type_code,
                `"${r.type_name}"`,
                `"${r.scores || ''}"`,
                `"${(r.user_agent || '').replace(/"/g, '""')}"`,
                r.created_at
            ].join(','))
        ].join('\n');
        
        res.setHeader('Content-Type', 'text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', `attachment; filename=diagnosis_${startDate}_${endDate}.csv`);
        res.send('\uFEFF' + csv);
    } catch (error) {
        console.error('Error exporting diagnosis:', error);
        res.status(500).json({ error: 'Failed to export diagnosis' });
    }
});

// CSVエクスポート - ページビュー
app.get('/api/admin/export/pageviews', adminAuth, async (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = (startDate || '2020-01-01') + 'T00:00:00';
        const end = (endDate || new Date().toISOString().split('T')[0]) + 'T23:59:59';
        
        const { data, error } = await supabase
            .from('page_views')
            .select('id, page, user_agent, referrer, ip, created_at')
            .gte('created_at', start)
            .lte('created_at', end)
            .order('created_at', { ascending: false });
        
        if (error) throw error;
        
        const headers = ['ID', 'ページ', 'ユーザーエージェント', 'リファラー', 'IP', '日時'];
        const csv = [
            headers.join(','),
            ...(data || []).map(r => [
                r.id,
                r.page,
                `"${(r.user_agent || '').replace(/"/g, '""')}"`,
                `"${(r.referrer || '').replace(/"/g, '""')}"`,
                `"${r.ip || ''}"`,
                r.created_at
            ].join(','))
        ].join('\n');
        
        res.setHeader('Content-Type', 'text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', `attachment; filename=pageviews_${startDate}_${endDate}.csv`);
        res.send('\uFEFF' + csv);
    } catch (error) {
        console.error('Error exporting pageviews:', error);
        res.status(500).json({ error: 'Failed to export pageviews' });
    }
});

// サーバー起動
app.listen(PORT, () => {
    console.log(`🚀 マリッジタイプ診断 管理サーバー起動（Supabase版）`);
    console.log(`   URL: http://localhost:${PORT}`);
    console.log(`   管理画面: http://localhost:${PORT}/admin`);
    console.log(`   データベース: Supabase (永続化対応)`);
});
