require('dotenv').config();
const express = require('express');
const axios = require('axios');
const cors = require('cors');

const app = express();
app.use(cors());

app.get('/alerts', async (req, res) => {
  try {
    // Bước 1: gửi query tới Trino
    const response = await axios.post(
      `http://${process.env.TRINO_HOST}:${process.env.TRINO_PORT}/v1/statement`,
      'SELECT event_id, camera_id AS location, timestamp_utc AS timestamp, frame_s3_path, label, score AS violence_score, \'Unreviewed\' AS status, \'v1.0\' AS model_version FROM iceberg.default.inference_results LIMIT 10',
      {
        headers: {
          'X-Trino-User': process.env.TRINO_USER,
          'X-Trino-Catalog': process.env.TRINO_CATALOG,
          'X-Trino-Schema': process.env.TRINO_SCHEMA,
        },
      }
    );

    // Bước 2: follow nextUri cho đến khi có data
    let data = [];
    let nextUri = response.data.nextUri;

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
      nextUri = nextRes.data.nextUri;
    }

    // Bước 3: map dữ liệu thành Alert object cho frontend
    const alerts = data.map(row => ({
      event_id: row[0],
      location: row[1],
      timestamp: row[2], // giữ nguyên string, frontend sẽ format
      frame_s3_path: row[3],
      label: row[4],
      violence_score: parseFloat(row[5]),
      status: row[6],
      model_version: row[7],
    }));

    res.json(alerts);
  } catch (err) {
    console.error('Trino query error:', err.message);
    if (err.response) {
      console.error('Trino response:', err.response.data);
    }
    res.status(500).json({ error: 'Query failed' });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Trino API running at http://localhost:${PORT}`);
});
