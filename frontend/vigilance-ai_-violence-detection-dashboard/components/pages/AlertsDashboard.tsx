import React, { useState, useMemo, useEffect } from 'react';
import { Alert, AlertStatus } from '../../types';
import { ChevronDown, ChevronUp, Camera, X } from 'lucide-react'; 

const getStatusPillColor = (status: AlertStatus) => {
  switch (status) {
    case 'Unreviewed':
      return 'bg-yellow-500/20 text-yellow-400';
    case 'Reviewed':
      return 'bg-blue-500/20 text-blue-400';
    case 'False Alarm':
      return 'bg-gray-500/20 text-gray-400';
    default:
      return 'bg-slate-500/20 text-slate-400';
  }
};

const getScoreColor = (score: number) => {
  if (score > 0.9) return 'text-red-400 font-bold';
  if (score > 0.75) return 'text-orange-400';
  return 'text-yellow-400';
};

const AlertModal: React.FC<{ alert: Alert; onClose: () => void }> = ({ alert, onClose }) => (
  <div className="fixed inset-0 bg-black/80 backdrop-blur-sm flex items-center justify-center z-50">
    <div className="relative bg-slate-900 rounded-xl shadow-2xl w-full max-w-2xl border border-slate-700">
      <div className="p-4 flex justify-between items-center border-b border-slate-800">
        <h3 className="text-xl font-bold text-white">Alert Details: {alert.event_id}</h3>
        <button onClick={onClose} className="p-2 rounded-full hover:bg-slate-800">
          <X size={20} />
        </button>
      </div>
      <div className="p-6">
        {/* Hiển thị ảnh từ MinIO */}
        <div className="aspect-video bg-black rounded-lg mb-4 flex items-center justify-center overflow-hidden">
          <img
            src={alert.frame_url}
            alt={`Frame ${alert.event_id}`}
            className="object-contain w-full h-full"
          />
        </div>
        <div className="grid grid-cols-2 gap-4 text-sm">
          <div><span className="font-semibold text-slate-400">Location:</span> {alert.location}</div>
          <div><span className="font-semibold text-slate-400">Timestamp:</span> {new Date(alert.timestamp).toLocaleString()}</div>
          <div><span className="font-semibold text-slate-400">Score:</span> <span className={getScoreColor(alert.violence_score)}>{alert.violence_score.toFixed(2)}</span></div>
          <div><span className="font-semibold text-slate-400">Label:</span> {alert.label}</div>
          <div><span className="font-semibold text-slate-400">Model:</span> {alert.model_version}</div>
          <div><span className="font-semibold text-slate-400">Status:</span> <span className={`px-2 py-1 rounded ${getStatusPillColor(alert.status)}`}>{alert.status}</span></div>
        </div>
      </div>
    </div>
  </div>
);

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
        .then((data) => setAlerts(data))
        .catch((err) => console.error('Failed to fetch alerts:', err));
    };

    fetchAlerts(); 
    const interval = setInterval(fetchAlerts, 60000); // gọi lại mỗi 60 giây

    return () => clearInterval(interval); // cleanup khi unmount
  }, []);

  const sortedAlerts = useMemo(() => {
    let sortableItems = [...alerts];
    if (sortConfig !== null) {
      sortableItems.sort((a, b) => {
        if (a[sortConfig.key] < b[sortConfig.key]) {
          return sortConfig.direction === 'asc' ? -1 : 1;
        }
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
    if (!sortConfig || sortConfig.key !== key) {
      return null;
    }
    return sortConfig.direction === 'asc' ? <ChevronUp size={16} /> : <ChevronDown size={16} />;
  };

  return (
    <div className="bg-slate-900/50 rounded-xl p-6 border border-slate-800">
      <div className="overflow-x-auto">
        <table className="w-full text-sm text-left text-slate-400">
          <thead className="text-xs text-slate-300 uppercase bg-slate-800/50">
            <tr>
              {['timestamp', 'location', 'violence_score', 'label', 'status'].map((key) => (
                <th key={key} scope="col" className="px-6 py-3" onClick={() => requestSort(key as keyof Alert)}>
                  <div className="flex items-center cursor-pointer">
                    {key.replace('_', ' ')}
                    <span className="ml-1">{getSortIcon(key as keyof Alert)}</span>
                  </div>
                </th>
              ))}
              <th scope="col" className="px-6 py-3">Image</th> 
            </tr>
          </thead>
          <tbody>
            {sortedAlerts.map((alert) => (
              <tr key={alert.event_id} className="border-b border-slate-800 hover:bg-slate-800/40 transition-colors">
                <td className="px-6 py-4 font-mono">{new Date(alert.timestamp).toLocaleString()}</td>
                <td className="px-6 py-4">{alert.location}</td>
                <td className={`px-6 py-4 font-mono ${getScoreColor(alert.violence_score)}`}>{alert.violence_score.toFixed(2)}</td>
                <td className="px-6 py-4">{alert.label}</td>
                <td className="px-6 py-4">
                  <span className={`px-2 py-1 text-xs font-medium rounded ${getStatusPillColor(alert.status)}`}>
                    {alert.status}
                  </span>
                </td>
                <td className="px-6 py-4">
                  <button onClick={() => setSelectedAlert(alert)} className="text-emerald-400 hover:text-emerald-300">
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
