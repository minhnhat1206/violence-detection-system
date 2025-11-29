import { Camera, CameraStatus, Alert, AlertStatus } from './types';
import { Video, BarChart2, Bell, MessageSquare, Settings as SettingsIcon } from 'lucide-react';

export const NAV_ITEMS = [
  { name: 'Live Streams', icon: Video },
  { name: 'Alerts Dashboard', icon: Bell },
  { name: 'Analytics', icon: BarChart2 },
  { name: 'Chatbot', icon: MessageSquare },
  { name: 'Settings', icon: SettingsIcon },
];

export const MOCK_CAMERAS: Camera[] = [
  { id: 'cam_01', city: 'TP. Hồ Chí Minh', district: 'Quận 1', ward: 'P. Bến Nghé', specificLocation: 'Phố đi bộ Nguyễn Huệ', status: CameraStatus.NORMAL, streamPath: 'cam_01' },
  { id: 'cam_02', city: 'TP. Hồ Chí Minh', district: 'Quận 1', ward: 'P. Bến Thành', specificLocation: 'Chợ Bến Thành', status: CameraStatus.VIOLENCE_DETECTED, streamPath: 'cam_02' },
  { id: 'cam_03', city: 'TP. Hồ Chí Minh', district: 'Quận 7', ward: 'P. Tân Phong', specificLocation: 'Cầu Ánh Sao', status: CameraStatus.NORMAL, streamPath: 'cam_03' },
  { id: 'cam_04', city: 'TP. Hồ Chí Minh', district: 'Quận Bình Thạnh', ward: 'P. 22', specificLocation: 'Landmark 81', status: CameraStatus.OFFLINE, streamPath: 'cam_04' },
  { id: 'cam_05', city: 'TP. Hồ Chí Minh', district: 'Quận 3', ward: 'P. Võ Thị Sáu', specificLocation: 'Dinh Độc Lập', status: CameraStatus.NORMAL, streamPath: 'cam_05' },
  { id: 'cam_06', city: 'TP. Hồ Chí Minh', district: 'TP. Thủ Đức', ward: 'P. Thảo Điền', specificLocation: 'Cầu Sài Gòn', status: CameraStatus.NORMAL, streamPath: 'cam_06' },
  { id: 'cam_07', city: 'TP. Hồ Chí Minh', district: 'Quận 1', ward: 'P. Phạm Ngũ Lão', specificLocation: 'Công viên 23/9', status: CameraStatus.NORMAL, streamPath: 'cam_07' },
  { id: 'cam_08', city: 'TP. Hồ Chí Minh', district: 'Quận 5', ward: 'P. 11', specificLocation: 'Chợ An Đông', status: CameraStatus.VIOLENCE_DETECTED, streamPath: 'cam_08' },
  { id: 'cam_09', city: 'TP. Hồ Chí Minh', district: 'Quận Tân Bình', ward: 'P. 2', specificLocation: 'Sân bay Tân Sơn Nhất', status: CameraStatus.NORMAL, streamPath: 'cam_09' },
  { id: 'cam_10', city: 'TP. Hồ Chí Minh', district: 'TP. Thủ Đức', ward: 'P. Hiệp Phú', specificLocation: 'Ngã tư Thủ Đức', status: CameraStatus.OFFLINE, streamPath: 'cam_10' },
  { id: 'cam_11', city: 'TP. Hồ Chí Minh', district: 'Quận 10', ward: 'P. 12', specificLocation: 'Vạn Hạnh Mall', status: CameraStatus.NORMAL, streamPath: 'cam_11' },
  { id: 'cam_12', city: 'TP. Hồ Chí Minh', district: 'Quận Gò Vấp', ward: 'P. 10', specificLocation: 'Ngã tư Phan Văn Trị', status: CameraStatus.NORMAL, streamPath: 'cam_12' },
];

const generateTimestamp = (daysAgo: number) => {
    const date = new Date();
    date.setDate(date.getDate() - daysAgo);
    date.setHours(Math.floor(Math.random() * 24), Math.floor(Math.random() * 60));
    return date.toISOString();
}

export const MOCK_ALERTS: Alert[] = [
  { event_id: 'EVT-1021', timestamp: generateTimestamp(0), location: 'Chợ An Đông', violence_score: 0.92, label: 'Fight', model_version: 'v2.1.3', clip_link: '#', status: 'Unreviewed' },
  { event_id: 'EVT-1020', timestamp: generateTimestamp(0), location: 'Chợ Bến Thành', violence_score: 0.88, label: 'Fight', model_version: 'v2.1.3', clip_link: '#', status: 'Unreviewed' },
  { event_id: 'EVT-1019', timestamp: generateTimestamp(1), location: 'Phố đi bộ Nguyễn Huệ', violence_score: 0.75, label: 'Crowd', model_version: 'v2.1.2', clip_link: '#', status: 'Reviewed' },
  { event_id: 'EVT-1018', timestamp: generateTimestamp(1), location: 'Cầu Sài Gòn', violence_score: 0.65, label: 'Anomaly', model_version: 'v2.1.3', clip_link: '#', status: 'Reviewed' },
  { event_id: 'EVT-1017', timestamp: generateTimestamp(2), location: 'Chợ An Đông', violence_score: 0.95, label: 'Fight', model_version: 'v2.1.2', clip_link: '#', status: 'False Alarm' },
  { event_id: 'EVT-1016', timestamp: generateTimestamp(2), location: 'Landmark 81', violence_score: 0.81, label: 'Crowd', model_version: 'v2.1.3', clip_link: '#', status: 'Reviewed' },
  { event_id: 'EVT-1015', timestamp: generateTimestamp(3), location: 'Phố đi bộ Nguyễn Huệ', violence_score: 0.78, label: 'Fight', model_version: 'v2.1.1', clip_link: '#', status: 'Reviewed' },
  { event_id: 'EVT-1014', timestamp: generateTimestamp(4), location: 'Cầu Ánh Sao', violence_score: 0.55, label: 'Anomaly', model_version: 'v2.1.1', clip_link: '#', status: 'Reviewed' },
  { event_id: 'EVT-1013', timestamp: generateTimestamp(5), location: 'Dinh Độc Lập', violence_score: 0.89, label: 'Fight', model_version: 'v2.1.1', clip_link: '#', status: 'False Alarm' },
  { event_id: 'EVT-1012', timestamp: generateTimestamp(6), location: 'Chợ An Đông', violence_score: 0.91, label: 'Fight', model_version: 'v2.1.1', clip_link: '#', status: 'Reviewed' },
];
