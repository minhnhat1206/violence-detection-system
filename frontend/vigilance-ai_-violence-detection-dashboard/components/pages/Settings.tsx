import React from 'react';
import { MOCK_CAMERAS } from '../../constants';
import { CameraStatus } from '../../types';
import { Trash2, Edit } from 'lucide-react';

const getStatusBadgeColor = (status: CameraStatus) => {
  switch (status) {
    case CameraStatus.NORMAL:
      return 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30';
    case CameraStatus.VIOLENCE_DETECTED:
      return 'bg-red-500/20 text-red-400 border-red-500/30';
    case CameraStatus.OFFLINE:
      return 'bg-slate-500/20 text-slate-400 border-slate-500/30';
    default:
      return '';
  }
};


const Settings: React.FC = () => {
  return (
    <div className="space-y-8 max-w-4xl mx-auto">
      <div className="bg-slate-900/50 p-6 rounded-xl border border-slate-800">
        <h3 className="text-xl font-semibold text-white mb-4">Alert Thresholds</h3>
        <form className="space-y-4">
          <div>
            <label htmlFor="violenceScore" className="block text-sm font-medium text-slate-400 mb-1">
              Violence Score Threshold
            </label>
            <input
              type="range"
              id="violenceScore"
              min="0.5"
              max="1.0"
              step="0.05"
              defaultValue="0.85"
              className="w-full h-2 bg-slate-700 rounded-lg appearance-none cursor-pointer accent-emerald-500"
            />
            <div className="flex justify-between text-xs text-slate-500 mt-1">
                <span>0.5</span>
                <span>0.85 (Current)</span>
                <span>1.0</span>
            </div>
          </div>
          <div>
            <label htmlFor="modelVersion" className="block text-sm font-medium text-slate-400 mb-1">
              Active Detection Model
            </label>
            <select
              id="modelVersion"
              className="w-full bg-slate-800 border border-slate-700 rounded-lg py-2 px-3 focus:outline-none focus:ring-2 focus:ring-emerald-500"
            >
              <option>v2.1.3 (Latest)</option>
              <option>v2.1.2 (Stable)</option>
              <option>v2.0.0 (Legacy)</option>
            </select>
          </div>
        </form>
      </div>

      <div className="bg-slate-900/50 p-6 rounded-xl border border-slate-800">
        <h3 className="text-xl font-semibold text-white mb-4">Manage Cameras</h3>
        
        <div className="mb-8 -mx-6 overflow-x-auto">
            <table className="w-full text-sm text-left text-slate-400">
                <thead className="text-xs text-slate-300 uppercase bg-slate-800/50">
                    <tr>
                        <th scope="col" className="px-6 py-3">ID</th>
                        <th scope="col" className="px-6 py-3">Location</th>
                        <th scope="col" className="px-6 py-3">Status</th>
                        <th scope="col" className="px-6 py-3 text-right">Actions</th>
                    </tr>
                </thead>
                <tbody>
                    {MOCK_CAMERAS.map((camera) => (
                        <tr key={camera.id} className="border-b border-slate-800 hover:bg-slate-800/40">
                            <td className="px-6 py-4 font-mono">{camera.id}</td>
                            <td className="px-6 py-4">{camera.specificLocation}, {camera.district}</td>
                            <td className="px-6 py-4">
                                <span className={`px-2 py-1 text-xs font-medium rounded-full border ${getStatusBadgeColor(camera.status)}`}>
                                    {camera.status}
                                </span>
                            </td>
                            <td className="px-6 py-4 text-right">
                                <button className="p-2 text-slate-400 hover:text-blue-400 transition-colors" aria-label={`Edit camera ${camera.id}`}>
                                    <Edit size={16} />
                                </button>
                                <button className="p-2 text-slate-400 hover:text-red-400 transition-colors" aria-label={`Delete camera ${camera.id}`}>
                                    <Trash2 size={16} />
                                </button>
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>

        <h4 className="text-lg font-semibold text-white mb-4 border-t border-slate-700 pt-6">Add New Camera</h4>
        <form className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
                 <label htmlFor="camId" className="block text-sm font-medium text-slate-400 mb-1">Camera ID</label>
                 <input type="text" id="camId" placeholder="e.g., CAM-013" className="w-full bg-slate-800 border border-slate-700 rounded-lg py-2 px-3 focus:outline-none focus:ring-2 focus:ring-emerald-500" />
            </div>
            <div>
                 <label htmlFor="location" className="block text-sm font-medium text-slate-400 mb-1">Location Name</label>
                 <input type="text" id="location" placeholder="e.g., City Park Entrance" className="w-full bg-slate-800 border border-slate-700 rounded-lg py-2 px-3 focus:outline-none focus:ring-2 focus:ring-emerald-500" />
            </div>
            <div className="md:col-span-2">
                 <label htmlFor="rtspUrl" className="block text-sm font-medium text-slate-400 mb-1">RTSP URL</label>
                 <input type="text" id="rtspUrl" placeholder="rtsp://..." className="w-full bg-slate-800 border border-slate-700 rounded-lg py-2 px-3 focus:outline-none focus:ring-2 focus:ring-emerald-500 font-mono" />
            </div>
             <div className="md:col-span-2">
                 <label htmlFor="description" className="block text-sm font-medium text-slate-400 mb-1">Description</label>
                 <textarea id="description" rows={3} placeholder="Notes about the camera placement or view." className="w-full bg-slate-800 border border-slate-700 rounded-lg py-2 px-3 focus:outline-none focus:ring-2 focus:ring-emerald-500"></textarea>
            </div>
            <div className="md:col-span-2 flex justify-end">
                <button type="submit" className="bg-emerald-600 hover:bg-emerald-500 text-white font-bold py-2 px-6 rounded-lg transition-all">
                    Add Camera
                </button>
            </div>
        </form>
      </div>
    </div>
  );
};

export default Settings;
