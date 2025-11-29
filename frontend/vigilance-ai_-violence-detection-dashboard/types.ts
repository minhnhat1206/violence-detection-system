export enum CameraStatus {
  NORMAL = 'Normal',
  VIOLENCE_DETECTED = 'Violence Detected',
  OFFLINE = 'Offline',
}

export interface Camera {
  id: string;
  city: string;
  district: string;
  ward: string;
  specificLocation: string;
  status: CameraStatus;
  streamPath: string;
}

export type AlertStatus = 'Unreviewed' | 'Reviewed' | 'False Alarm';

export interface Alert {
  event_id: string;
  timestamp: string;
  location: string;
  violence_score: number;
  label: 'Fight' | 'Crowd' | 'Anomaly';
  model_version: string;
  clip_link: string;
  status: AlertStatus;
}

export interface ChatMessage {
  role: 'user' | 'model';
  content: string;
}
