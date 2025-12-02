import React, { useEffect, useState, useMemo } from 'react';
import { 
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, 
  BarChart, Bar, Legend, LineChart, Line, ComposedChart 
} from 'recharts';
import { ShieldAlert, Activity, MapPin, Video } from 'lucide-react';

interface AnalyticData {
  timestamp: string;
  district: string;
  ward: string;
  camera_id: string;
  is_violent: boolean;
  risk_score: number;
  duration: number;
  fps: number;
  latency: number;
  alert_count: number;
}

const KPICard = ({ title, value, icon: Icon, color }: any) => (
  <div className="bg-slate-800/50 p-4 rounded-xl border border-slate-700 backdrop-blur-sm">
    <div className="flex justify-between items-start">
      <div>
        <p className="text-slate-400 text-sm font-medium">{title}</p>
        <h3 className="text-2xl font-bold text-white mt-1">{value}</h3>
      </div>
      <div className={`p-2 rounded-lg ${color}`}>
        <Icon size={20} className="text-white" />
      </div>
    </div>
  </div>
);

const CustomTooltip = ({ active, payload, label }: any) => {
  if (active && payload && payload.length) {
    return (
      <div className="bg-slate-900 border border-slate-700 p-3 rounded-lg shadow-xl text-sm">
        <p className="text-slate-300 font-mono mb-2">{new Date(label).toLocaleTimeString()}</p>
        {payload.map((p: any, index: number) => (
          <p key={index} style={{ color: p.color }}>
            {p.name}: <span className="font-bold">{Number(p.value).toFixed(2)}</span>
          </p>
        ))}
      </div>
    );
  }
  return null;
};

const Analytics: React.FC = () => {
  const [data, setData] = useState<AnalyticData[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('http://localhost:3000/analytics')
      .then(res => res.json())
      .then(fetchedData => {
        // Sort theo thời gian tăng dần để vẽ biểu đồ cho đúng
        const sorted = fetchedData.sort((a: any, b: any) => 
          new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
        );
        setData(sorted);
        setLoading(false);
      })
      .catch(err => console.error(err));
  }, []);

  // --- DATA PROCESSING FOR CHARTS ---

  // 1. Dữ liệu cho biểu đồ xu hướng (Timeline)
  const timeSeriesData = useMemo(() => {
    return data.map(item => ({
      ...item,
      // Tạo điểm số bạo lực (chỉ hiển thị khi có violent, nếu ko thì 0)
      violence_intensity: item.is_violent ? item.risk_score : 0,
      baseline: 0.5 // Đường tham chiếu
    }));
  }, [data]);

  // 2. Dữ liệu tổng hợp theo Quận (Top High Risk Areas)
  const districtStats = useMemo(() => {
    const stats: Record<string, number> = {};
    data.forEach(item => {
      if (item.is_violent) {
        stats[item.district] = (stats[item.district] || 0) + 1;
      }
    });
    return Object.entries(stats)
      .map(([name, count]) => ({ name, count }))
      .sort((a, b) => b.count - a.count);
  }, [data]);

  // 3. KPI Calculations
  const totalIncidents = data.filter(d => d.is_violent).length;
  const avgFPS = data.length > 0 ? (data.reduce((acc, curr) => acc + curr.fps, 0) / data.length).toFixed(1) : 0;
  const maxRisk = data.length > 0 ? Math.max(...data.map(d => d.risk_score)).toFixed(4) : 0;
  const activeCameras = new Set(data.map(d => d.camera_id)).size;

  if (loading) return <div className="text-white text-center p-10">Loading Analytics...</div>;

  return (
    <div className="space-y-6">
      {/* 1. KPI SECTION */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <KPICard title="Total Violence Incidents" value={totalIncidents} icon={ShieldAlert} color="bg-red-500" />
        <KPICard title="Avg System FPS" value={avgFPS} icon={Activity} color="bg-blue-500" />
        <KPICard title="Peak Risk Score" value={maxRisk} icon={Activity} color="bg-orange-500" />
        <KPICard title="Active Cameras" value={activeCameras} icon={Video} color="bg-emerald-500" />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        
        {/* 2. MAIN CHART: VIOLENCE TREND (Area Chart) */}
        <div className="lg:col-span-2 bg-slate-900/80 border border-slate-800 p-6 rounded-xl">
          <div className="flex justify-between items-center mb-6">
            <div>
              <h3 className="text-lg font-bold text-white">Violence Intensity Timeline</h3>
              <p className="text-xs text-slate-400">Real-time risk score analysis over time</p>
            </div>
            <div className="flex gap-2">
                <span className="flex items-center text-xs text-slate-400"><div className="w-2 h-2 bg-red-500 rounded-full mr-2"></div> Detected</span>
                <span className="flex items-center text-xs text-slate-400"><div className="w-2 h-2 bg-emerald-500 rounded-full mr-2"></div> Normal</span>
            </div>
          </div>
          
          <div className="h-[300px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={timeSeriesData}>
                <defs>
                  <linearGradient id="colorRisk" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#ef4444" stopOpacity={0.8}/>
                    <stop offset="95%" stopColor="#ef4444" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#334155" opacity={0.5} />
                <XAxis 
                  dataKey="timestamp" 
                  tickFormatter={(str) => new Date(str).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'})}
                  stroke="#94a3b8" 
                  fontSize={12}
                />
                <YAxis stroke="#94a3b8" fontSize={12} domain={[0, 1]} />
                <Tooltip content={<CustomTooltip />} />
                <Area 
                  type="monotone" 
                  dataKey="violence_intensity" 
                  stroke="#ef4444" 
                  fillOpacity={1} 
                  fill="url(#colorRisk)" 
                  name="Risk Score"
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* 3. LOCATION CHART: BAR CHART */}
        <div className="bg-slate-900/80 border border-slate-800 p-6 rounded-xl">
          <div className="mb-6">
            <h3 className="text-lg font-bold text-white">Hotspots by District</h3>
            <p className="text-xs text-slate-400">Number of incidents per location</p>
          </div>
          <div className="h-[300px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={districtStats} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" stroke="#334155" horizontal={false} opacity={0.5} />
                <XAxis type="number" stroke="#94a3b8" hide />
                <YAxis dataKey="name" type="category" stroke="#f8fafc" width={100} fontSize={12} />
                <Tooltip 
                  cursor={{fill: '#334155', opacity: 0.4}}
                  contentStyle={{ backgroundColor: '#0f172a', borderColor: '#334155', color: '#fff' }}
                />
                <Bar dataKey="count" fill="#f59e0b" radius={[0, 4, 4, 0]} barSize={20} name="Incidents" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* 4. SYSTEM HEALTH: COMPOSED CHART (Line + Area) */}
      <div className="bg-slate-900/80 border border-slate-800 p-6 rounded-xl">
        <div className="mb-6">
          <h3 className="text-lg font-bold text-white">System Performance Health</h3>
          <p className="text-xs text-slate-400">Monitoring FPS stability and Network Latency</p>
        </div>
        <div className="h-[250px] w-full">
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={timeSeriesData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" opacity={0.3} />
              <XAxis 
                dataKey="timestamp" 
                tickFormatter={(str) => new Date(str).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'})}
                stroke="#94a3b8" 
                fontSize={12}
                minTickGap={50}
              />
              <YAxis yAxisId="left" stroke="#3b82f6" fontSize={12} label={{ value: 'FPS', angle: -90, position: 'insideLeft', fill: '#3b82f6' }} />
              <YAxis yAxisId="right" orientation="right" stroke="#10b981" fontSize={12} label={{ value: 'Latency (ms)', angle: 90, position: 'insideRight', fill: '#10b981' }} />
              <Tooltip content={<CustomTooltip />} />
              <Legend />
              <Line yAxisId="left" type="monotone" dataKey="fps" stroke="#3b82f6" strokeWidth={2} dot={false} name="FPS" />
              <Area yAxisId="right" type="monotone" dataKey="latency" fill="#10b981" stroke="#10b981" fillOpacity={0.1} name="Latency (ms)" />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
};

export default Analytics;