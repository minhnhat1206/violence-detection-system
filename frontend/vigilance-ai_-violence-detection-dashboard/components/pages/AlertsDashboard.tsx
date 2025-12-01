import React, { useState, useMemo, useEffect } from 'react';
import { ChevronDown, ChevronUp, Camera, X } from 'lucide-react';

// 1. ĐỊNH NGHĨA LẠI TYPES CHO KHỚP API
export type AlertStatus = 'Unreviewed' | 'Reviewed' | 'False Alarm';

export interface Alert {
  event_id: string;
  location: string;       // "Cam01 - Nguyen Hue..."
  timestamp: string;
  frame_url: string;      // Link ảnh từ MinIO
  label: string;
  violence_score: number; // Lưu ý: Chuyển về number để sort/tô màu
  status: AlertStatus;
  model_version: string;
}

// Helper: Màu sắc cho trạng thái
const getStatusPillColor = (status: AlertStatus) => {
  switch (status) {
    case 'Unreviewed':
      return 'bg-yellow-500/20 text-yellow-400 border border-yellow-500/30';
    case 'Reviewed':
      return 'bg-blue-500/20 text-blue-400 border border-blue-500/30';
    case 'False Alarm':
      return 'bg-gray-500/20 text-gray-400 border border-gray-500/30';
    default:
      return 'bg-slate-500/20 text-slate-400';
  }
};

// Helper: Màu sắc cho điểm số
const getScoreColor = (score: number) => {
  if (score >= 0.9) return 'text-red-500 font-bold'; // Rất cao
  if (score >= 0.75) return 'text-orange-400 font-semibold'; // Cao
  return 'text-yellow-400'; // Trung bình
};

// --- COMPONENT: MODAL CHI TIẾT ---
const AlertModal: React.FC<{ alert: Alert; onClose: () => void }> = ({ alert, onClose }) => (
  <div className="fixed inset-0 bg-black/90 backdrop-blur-sm flex items-center justify-center z-50 p-4">
    <div className="relative bg-slate-900 rounded-2xl shadow-2xl w-full max-w-3xl border border-slate-700 flex flex-col max-h-[90vh]">
      
      {/* Header */}
      <div className="p-4 flex justify-between items-center border-b border-slate-800 shrink-0">
        <div>
          <h3 className="text-xl font-bold text-white tracking-tight">Event Details</h3>
          <p className="text-xs text-slate-500 font-mono mt-1">{alert.event_id}</p>
        </div>
        <button onClick={onClose} className="p-2 rounded-full hover:bg-slate-800 text-slate-400 hover:text-white transition-colors">
          <X size={24} />
        </button>
      </div>

      {/* Body */}
      <div className="p-6 overflow-y-auto">
        {/* Ảnh Evidence */}
        <div className="aspect-video bg-black rounded-xl mb-6 flex items-center justify-center overflow-hidden border border-slate-800 group relative">
          <img
            src={alert.frame_url}
            alt={`Evidence ${alert.event_id}`}
            className="object-contain w-full h-full"
            onError={(e) => {
                (e.target as HTMLImageElement).src = 'https://via.placeholder.com/640x360?text=Image+Not+Found';
            }}
          />
          <div className="absolute inset-0 bg-gradient-to-t from-black/60 to-transparent opacity-0 group-hover:opacity-100 transition-opacity flex items-end p-4">
            <span className="text-white text-sm font-medium">Evidence captured at {new Date(alert.timestamp).toLocaleTimeString()}</span>
          </div>
        </div>

        {/* Thông tin chi tiết */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 text-sm">
          <div className="space-y-4">
            <div>
              <span className="block text-xs font-semibold text-slate-500 uppercase tracking-wider mb-1">Location</span>
              <div className="text-slate-200 font-medium">{alert.location}</div>
            </div>
            <div>
              <span className="block text-xs font-semibold text-slate-500 uppercase tracking-wider mb-1">Timestamp</span>
              <div className="text-slate-200 font-mono">{new Date(alert.timestamp).toLocaleString()}</div>
            </div>
            <div>
               <span className="block text-xs font-semibold text-slate-500 uppercase tracking-wider mb-1">AI Model</span>
               <div className="text-slate-200">{alert.model_version}</div>
            </div>
          </div>

          <div className="space-y-4">
            <div>
              <span className="block text-xs font-semibold text-slate-500 uppercase tracking-wider mb-1">Violence Score</span>
              <div className={`text-2xl ${getScoreColor(alert.violence_score)}`}>
                {(alert.violence_score * 100).toFixed(2)}%
              </div>
            </div>
            <div>
              <span className="block text-xs font-semibold text-slate-500 uppercase tracking-wider mb-1">Classification</span>
              <div className="text-slate-200">{alert.label}</div>
            </div>
            <div>
               <span className="block text-xs font-semibold text-slate-500 uppercase tracking-wider mb-1">Review Status</span>
               <span className={`inline-block px-2 py-1 rounded text-xs font-medium ${getStatusPillColor(alert.status)}`}>
                  {alert.status}
               </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
);

// --- COMPONENT: DASHBOARD ---
const AlertsDashboard: React.FC = () => {
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [selectedAlert, setSelectedAlert] = useState<Alert | null>(null);
  const [sortConfig, setSortConfig] = useState<{ key: keyof Alert; direction: 'asc' | 'desc' } | null>({
    key: 'timestamp',
    direction: 'desc',
  });

  useEffect(() => {
    const fetchAlerts = () => {
      fetch('http://localhost:3000/alerts')
        .then((res) => res.json())
        .then((data: any[]) => {
            // Transform dữ liệu: Chuyển violence_score từ string sang number để sort đúng
            const formattedData: Alert[] = data.map(item => ({
                ...item,
                violence_score: Number(item.violence_score) // Quan trọng!
            }));
            setAlerts(formattedData);
        })
        .catch((err) => console.error('Failed to fetch alerts:', err));
    };

    fetchAlerts(); 
    const interval = setInterval(fetchAlerts, 10000); // Poll mỗi 10s cho nhanh thấy

    return () => clearInterval(interval);
  }, []);

  const sortedAlerts = useMemo(() => {
    let sortableItems = [...alerts];
    if (sortConfig !== null) {
      sortableItems.sort((a, b) => {
        // @ts-ignore
        if (a[sortConfig.key] < b[sortConfig.key]) {
          return sortConfig.direction === 'asc' ? -1 : 1;
        }
        // @ts-ignore
        if (a[sortConfig.key] > b[sortConfig.key]) {
          return sortConfig.direction === 'asc' ? 1 : -1;
        }
        return 0;
      });
    }
    return sortableItems;
  }, [alerts, sortConfig]);

  const requestSort = (key: keyof Alert) => {
    let direction: 'asc' | 'desc' = 'asc';
    if (sortConfig && sortConfig.key === key && sortConfig.direction === 'asc') {
      direction = 'desc';
    }
    setSortConfig({ key, direction });
  };

  const getSortIcon = (key: keyof Alert) => {
    if (!sortConfig || sortConfig.key !== key) return null;
    return sortConfig.direction === 'asc' ? <ChevronUp size={16} /> : <ChevronDown size={16} />;
  };

  return (
    <div className="bg-slate-900/80 backdrop-blur rounded-xl border border-slate-800 shadow-xl overflow-hidden">
      <div className="p-6 border-b border-slate-800">
        <h2 className="text-lg font-semibold text-white">Recent Security Alerts (Star Schema Data)</h2>
        <p className="text-sm text-slate-400">Real-time monitoring from Kafka & Iceberg Lakehouse</p>
      </div>
      
      <div className="overflow-x-auto">
        <table className="w-full text-sm text-left text-slate-400">
          <thead className="text-xs text-slate-300 uppercase bg-slate-800/80 sticky top-0">
            <tr>
              {['timestamp', 'location', 'violence_score', 'label', 'status'].map((key) => (
                <th key={key} scope="col" className="px-6 py-4 cursor-pointer hover:bg-slate-800 transition-colors" onClick={() => requestSort(key as keyof Alert)}>
                  <div className="flex items-center gap-2">
                    {key.replace('_', ' ')}
                    <span className="text-slate-500">{getSortIcon(key as keyof Alert)}</span>
                  </div>
                </th>
              ))}
              <th scope="col" className="px-6 py-4 text-center">Evidence</th> 
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800">
            {sortedAlerts.length === 0 ? (
                <tr>
                    <td colSpan={6} className="px-6 py-8 text-center text-slate-500 italic">
                        No alerts found in the Data Warehouse.
                    </td>
                </tr>
            ) : sortedAlerts.map((alert) => (
              <tr key={alert.event_id} className="hover:bg-slate-800/40 transition-colors group">
                <td className="px-6 py-4 font-mono text-slate-300">
                    {new Date(alert.timestamp).toLocaleString()}
                </td>
                <td className="px-6 py-4 font-medium text-white">
                    {alert.location}
                </td>
                <td className="px-6 py-4">
                    <div className={`font-mono font-bold ${getScoreColor(alert.violence_score)}`}>
                        {alert.violence_score.toFixed(4)}
                    </div>
                </td>
                <td className="px-6 py-4">{alert.label}</td>
                <td className="px-6 py-4">
                  <span className={`px-2.5 py-1 text-xs font-semibold rounded-full ${getStatusPillColor(alert.status)}`}>
                    {alert.status}
                  </span>
                </td>
                <td className="px-6 py-4 text-center">
                  <button 
                    onClick={() => setSelectedAlert(alert)} 
                    className="p-2 bg-slate-800 text-emerald-400 rounded-lg hover:bg-emerald-500/20 hover:text-emerald-300 transition-all transform hover:scale-105"
                    title="View Evidence"
                  >
                    <Camera size={18} /> 
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {selectedAlert && <AlertModal alert={selectedAlert} onClose={() => setSelectedAlert(null)} />}
    </div>
  );
};

export default AlertsDashboard;