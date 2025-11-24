import React, { useState, useEffect } from 'react';
import { MOCK_CAMERAS } from '../../constants';
import { Camera, CameraStatus } from '../../types';
import { X, Maximize } from 'lucide-react';
import WebRTCPlayer from '../common/WebRTCPlayer';

const getStatusBadgeColor = (status: CameraStatus) => {
  switch (status) {
    case CameraStatus.NORMAL:
      return 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30';
    case CameraStatus.VIOLENCE_DETECTED:
      return 'bg-red-500/20 text-red-400 border-red-500/30 animate-pulse';
    case CameraStatus.OFFLINE:
      return 'bg-slate-500/20 text-slate-400 border-slate-500/30';
    default:
      return '';
  }
};

const CameraCard: React.FC<{ camera: Camera; onFocus: (camera: Camera) => void }> = ({ camera, onFocus }) => {
    const [time, setTime] = useState(new Date());

    useEffect(() => {
        const timer = setInterval(() => setTime(new Date()), 1000);
        return () => clearInterval(timer);
    }, []);

    return (
        <div className="bg-slate-900/50 rounded-xl overflow-hidden shadow-lg border border-slate-800 hover:border-emerald-500/50 transition-all duration-300 group">
            <div className="relative aspect-video">
                {camera.status !== CameraStatus.OFFLINE ? (
                    <WebRTCPlayer streamPath={camera.streamPath} />
                ) : (
                    <div className="w-full h-full bg-black flex items-center justify-center">
                        <p className="text-slate-500">OFFLINE</p>
                    </div>
                )}
                <div className="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity">
                    <button onClick={() => onFocus(camera)} className="p-2 bg-slate-900/50 rounded-full text-white hover:bg-emerald-500">
                        <Maximize size={16} />
                    </button>
                </div>
                <div className="absolute bottom-0 left-0 w-full p-2 bg-gradient-to-t from-black/60 to-transparent">
                     <p className="text-white font-semibold text-sm drop-shadow-md">{camera.specificLocation}</p>
                     <p className="text-slate-300 text-xs drop-shadow-md">{`${camera.ward}, ${camera.district}`}</p>
                </div>
            </div>
            <div className="p-3 flex justify-between items-center">
                <span className={`px-2 py-1 text-xs font-medium rounded-full border ${getStatusBadgeColor(camera.status)}`}>
                    {camera.status}
                </span>
                <span className="text-xs text-slate-400 font-mono">{time.toLocaleTimeString()}</span>
            </div>
        </div>
    )
}

const FocusModal: React.FC<{ camera: Camera; onClose: () => void }> = ({ camera, onClose }) => (
    <div className="fixed inset-0 bg-black/80 backdrop-blur-sm flex items-center justify-center z-50">
        <div className="relative bg-slate-900 rounded-xl shadow-2xl w-full max-w-4xl border border-slate-700 overflow-hidden">
             <div className="p-4 flex justify-between items-center border-b border-slate-800">
                <div>
                    <h3 className="text-xl font-bold text-white">{camera.specificLocation}</h3>
                    <p className="text-sm text-slate-400">{`${camera.ward}, ${camera.district}, ${camera.city}`}</p>
                </div>
                <button onClick={onClose} className="p-2 rounded-full hover:bg-slate-800">
                    <X size={20} />
                </button>
            </div>
            <div className="aspect-video bg-black">
                {camera.status !== CameraStatus.OFFLINE ? (
                     <WebRTCPlayer streamPath={camera.streamPath} isMuted={false} />
                ) : (
                    <div className="w-full h-full bg-black flex items-center justify-center">
                        <p className="text-slate-500 text-lg">CAMERA OFFLINE</p>
                    </div>
                )}
            </div>
            <div className="p-4 bg-slate-900/50 flex justify-between items-center">
                 <span className={`px-3 py-1 text-sm font-medium rounded-full border ${getStatusBadgeColor(camera.status)}`}>
                    {camera.status}
                </span>
                <span className="text-sm text-slate-400 font-mono">{new Date().toLocaleString()}</span>
            </div>
        </div>
    </div>
);

const LiveStreams: React.FC = () => {
  const [cameras] = useState<Camera[]>(MOCK_CAMERAS);
  const [focusedCamera, setFocusedCamera] = useState<Camera | null>(null);

  return (
    <div>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
            {cameras.map((camera) => (
                <CameraCard key={camera.id} camera={camera} onFocus={setFocusedCamera} />
            ))}
        </div>
        {focusedCamera && <FocusModal camera={focusedCamera} onClose={() => setFocusedCamera(null)} />}
    </div>
  );
};

export default LiveStreams;
