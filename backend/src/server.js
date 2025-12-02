require('dotenv').config();
const express = require('express');
const axios = require('axios');
const cors = require('cors');

const app = express();

// Bật CORS để Frontend React gọi được API
app.use(cors());

// Hàm helper để gửi query tới Trino và xử lý pagination (NextUri)
// Giúp code gọn hơn, đỡ phải viết lặp lại logic while loop
async function executeTrinoQuery(query) {
  // 1. Gửi query ban đầu
  const response = await axios.post(
    `http://${process.env.TRINO_HOST}:${process.env.TRINO_PORT}/v1/statement`,
    query,
    {
      headers: {
        'X-Trino-User': process.env.TRINO_USER,
        'X-Trino-Catalog': process.env.TRINO_CATALOG,
        'X-Trino-Schema': process.env.TRINO_SCHEMA,
      },
    }
  );

  let data = [];
  let nextUri = response.data.nextUri;
  
  // Lấy data ngay từ response đầu tiên (nếu query chạy cực nhanh)
  if (response.data.data) {
    data = data.concat(response.data.data);
  }

  // 2. Loop để lấy hết các trang kết quả (Pagination)
  while (nextUri) {
    const nextRes = await axios.get(nextUri, {
      headers: {
        'X-Trino-User': process.env.TRINO_USER,
        'X-Trino-Catalog': process.env.TRINO_CATALOG,
        'X-Trino-Schema': process.env.TRINO_SCHEMA,
      },
    });
    
    if (nextRes.data.data) {
      data = data.concat(nextRes.data.data);
    }
    
    // Nếu có lỗi SQL, Trino sẽ trả về object error
    if (nextRes.data.error) {
       console.error("Trino SQL Error:", nextRes.data.error);
       throw new Error(`Trino Error: ${nextRes.data.error.message}`);
    }

    nextUri = nextRes.data.nextUri;
  }
  
  return data;
}

// =======================================================
// API 1: ANALYTICS (Dữ liệu biểu đồ)
// =======================================================
app.get('/analytics', async (req, res) => {
  try {
    const query = `
      SELECT 
        f.window_start,
        l.district,
        l.ward,
        c.camera_id,
        f.is_violent_window,
        f.max_risk_score,
        f.total_duration_sec,
        f.avg_fps,
        f.avg_latency_ms,
        f.alert_count
      FROM iceberg.default.fact_camera_monitoring AS f
      LEFT JOIN iceberg.default.dim_location AS l ON f.location_key = l.location_key
      LEFT JOIN iceberg.default.dim_camera AS c ON f.camera_key = c.camera_key
      ORDER BY f.window_start DESC
      LIMIT 1000
    `;

    const rawData = await executeTrinoQuery(query);

    // Map dữ liệu cho Frontend Recharts
    const analyticsData = rawData.map(row => ({
      timestamp: row[0],
      district: row[1] || 'Unknown',
      ward: row[2] || 'Unknown',
      camera_id: row[3],
      is_violent: row[4],
      risk_score: row[5] ? parseFloat(row[5]) : 0,
      duration: row[6] ? parseFloat(row[6]) : 0,
      fps: row[7] ? parseFloat(row[7]) : 0,
      latency: row[8] ? parseFloat(row[8]) : 0,
      alert_count: row[9] ? parseInt(row[9]) : 0
    }));

    res.json(analyticsData);

  } catch (err) {
    console.error('Analytics endpoint error:', err.message);
    res.status(500).json({ error: 'Analytics Query failed' });
  }
});

// =======================================================
// API 2: ALERTS (Danh sách cảnh báo Live)
// =======================================================
app.get('/alerts', async (req, res) => {
  try {
    const query = `
      SELECT 
        f.fact_id, 
        c.camera_id, 
        l.street, 
        l.district, 
        f.window_start, 
        f.evidence_url, 
        f.max_risk_score
      FROM iceberg.default.fact_camera_monitoring AS f
      LEFT JOIN iceberg.default.dim_camera AS c ON f.camera_key = c.camera_key
      LEFT JOIN iceberg.default.dim_location AS l ON f.location_key = l.location_key
      WHERE f.is_violent_window = true
      ORDER BY f.window_start DESC
      LIMIT 20
    `;

    const rawData = await executeTrinoQuery(query);

    // Map dữ liệu cho Frontend Table
    const alerts = rawData.map(row => {
        const cameraId = row[1] || 'Unknown Cam';
        const street = row[2] || 'Unknown Street';
        const district = row[3] || '';
        const evidenceUrl = row[5];

        return {
            event_id: row[0],
            location: `${cameraId} - ${street}, ${district}`, 
            timestamp: row[4], 
            
            // Xử lý link ảnh (Fallback nếu null)
            frame_url: evidenceUrl ? evidenceUrl : 'https://via.placeholder.com/640x360?text=No+Evidence',
            
            label: 'Violence Detected',
            violence_score: parseFloat(row[6]).toFixed(4), 
            status: 'Unreviewed',
            model_version: 'MoViNet-A3'
        };
    });

    res.json(alerts);

  } catch (err) {
    console.error('Alerts endpoint error:', err.message);
    res.status(500).json({ error: 'Alerts Query failed' });
  }
});

// Khởi động Server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 UrbanSafety API running at http://localhost:${PORT}`);
  console.log(`   - Alerts: http://localhost:${PORT}/alerts`);
  console.log(`   - Analytics: http://localhost:${PORT}/analytics`);
});