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

// 静的ファイル配信
app.use('/admin', express.static(path.join(__dirname, 'admin-panel')));

// ヘルスチェック（Render用）
app.get('/api/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString(), database: 'supabase' });
});

// ==========================================
// API: データ記録
// ==========================================

// ページビュー記録
app.post('/api/track/pageview', async (req, res) => {
    try {
        const { page } = req.body;
        const userAgent = req.headers['user-agent'] || '';
        const referrer = req.headers['referer'] || '';
        const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress || '';
        
        const { error } = await supabase
            .from('page_views')
            .insert({ page, user_agent: userAgent, referrer, ip });
        
        if (error) throw error;
        res.json({ success: true });
    } catch (error) {
        console.error('Error tracking pageview:', error);
        res.status(500).json({ error: 'Failed to track pageview' });
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
        const today = new Date().toISOString().split('T')[0];
        const todayStart = today + 'T00:00:00';
        const todayEnd = today + 'T23:59:59';
        
        // 総アクセス数
        const { count: totalViews } = await supabase
            .from('page_views')
            .select('*', { count: 'exact', head: true });
        
        // 今日のアクセス数
        const { count: todayViews } = await supabase
            .from('page_views')
            .select('*', { count: 'exact', head: true })
            .gte('created_at', todayStart)
            .lte('created_at', todayEnd);
        
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
        
        // 過去7日間のアクセス推移
        const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
        const { data: viewsData } = await supabase
            .from('page_views')
            .select('created_at')
            .gte('created_at', sevenDaysAgo);
        
        const dailyViews = {};
        (viewsData || []).forEach(v => {
            const date = v.created_at.split('T')[0];
            dailyViews[date] = (dailyViews[date] || 0) + 1;
        });
        const dailyViewsArray = Object.entries(dailyViews)
            .map(([date, count]) => ({ date, count }))
            .sort((a, b) => a.date.localeCompare(b.date));
        
        // 過去7日間の診断数推移
        const { data: diagData } = await supabase
            .from('diagnosis_results')
            .select('created_at')
            .gte('created_at', sevenDaysAgo);
        
        const dailyDiagnosis = {};
        (diagData || []).forEach(d => {
            const date = d.created_at.split('T')[0];
            dailyDiagnosis[date] = (dailyDiagnosis[date] || 0) + 1;
        });
        const dailyDiagnosisArray = Object.entries(dailyDiagnosis)
            .map(([date, count]) => ({ date, count }))
            .sort((a, b) => a.date.localeCompare(b.date));
        
        res.json({
            totalViews: totalViews || 0,
            todayViews: todayViews || 0,
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

// ==========================================
// API: 高度な分析機能
// ==========================================

// 期間指定ダッシュボード
app.get('/api/admin/analytics', adminAuth, async (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        const start = (startDate || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0]) + 'T00:00:00';
        const end = (endDate || new Date().toISOString().split('T')[0]) + 'T23:59:59';
        
        // 期間内のアクセス数
        const { count: periodViews } = await supabase
            .from('page_views')
            .select('*', { count: 'exact', head: true })
            .gte('created_at', start)
            .lte('created_at', end);
        
        // 期間内の診断数
        const { count: periodDiagnosis } = await supabase
            .from('diagnosis_results')
            .select('*', { count: 'exact', head: true })
            .gte('created_at', start)
            .lte('created_at', end);
        
        // 日別アクセス推移
        const { data: viewsData } = await supabase
            .from('page_views')
            .select('created_at')
            .gte('created_at', start)
            .lte('created_at', end);
        
        const dailyViews = {};
        (viewsData || []).forEach(v => {
            const date = v.created_at.split('T')[0];
            dailyViews[date] = (dailyViews[date] || 0) + 1;
        });
        const dailyViewsArray = Object.entries(dailyViews)
            .map(([date, count]) => ({ date, count }))
            .sort((a, b) => a.date.localeCompare(b.date));
        
        // 日別診断推移
        const { data: diagData } = await supabase
            .from('diagnosis_results')
            .select('created_at, type_code, type_name')
            .gte('created_at', start)
            .lte('created_at', end);
        
        const dailyDiagnosis = {};
        const typeStats = {};
        (diagData || []).forEach(d => {
            const date = d.created_at.split('T')[0];
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
            periodDiagnosis: periodDiagnosis || 0,
            dailyViews: dailyViewsArray,
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
        
        (viewsData || []).forEach(v => {
            const hour = new Date(v.created_at).getUTCHours().toString().padStart(2, '0');
            hourlyViews[hour] = (hourlyViews[hour] || 0) + 1;
        });
        
        (diagData || []).forEach(d => {
            const hour = new Date(d.created_at).getUTCHours().toString().padStart(2, '0');
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
        
        (viewsData || []).forEach(v => {
            const weekday = new Date(v.created_at).getDay().toString();
            weekdayViews[weekday] = (weekdayViews[weekday] || 0) + 1;
        });
        
        (diagData || []).forEach(d => {
            const weekday = new Date(d.created_at).getDay().toString();
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
        
        (data || []).forEach(row => {
            const date = new Date(row.created_at);
            const weekday = date.getDay();
            const hour = date.getUTCHours();
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
