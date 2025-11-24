import React, { useEffect, useRef, useState } from 'react';
import { Loader2, VideoOff } from 'lucide-react';

interface WebRTCPlayerProps {
  streamPath: string;
  isMuted?: boolean;
}

const WebRTCPlayer: React.FC<WebRTCPlayerProps> = ({ streamPath, isMuted = true }) => {
  const videoRef = useRef<HTMLVideoElement>(null);
  const [status, setStatus] = useState<'loading' | 'playing' | 'error'>('loading');

  useEffect(() => {
    if (!streamPath) {
      setStatus('error');
      return;
    }

    let isCancelled = false;
    let sessionUrl = '';
    const pc = new RTCPeerConnection();

    // Create a MediaStream and attach it to the video element immediately.
    // Tracks will be added to this stream as they are received.
    const remoteStream = new MediaStream();
    if (videoRef.current) {
        videoRef.current.srcObject = remoteStream;
    }

    pc.ontrack = (event) => {
        // Add the received track to the stream that's already attached to the video element.
        remoteStream.addTrack(event.track);
    };

    pc.onconnectionstatechange = () => {
      if (isCancelled) return;
      switch(pc.connectionState) {
        case 'connected':
          // The connection has become fully connected
          break;
        case 'disconnected':
        case 'failed':
        case 'closed':
          setStatus('error');
          break;
      }
    };
    
    const connect = async () => {
      try {
        if (isCancelled) return;
        setStatus('loading');

        pc.addTransceiver('video', { direction: 'recvonly' });
        pc.addTransceiver('audio', { direction: 'recvonly' });

        const offer = await pc.createOffer();
        await pc.setLocalDescription(offer);

        const whepUrl = `http://${window.location.hostname}:8889/${streamPath}/whep`;
        const response = await fetch(whepUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/sdp' },
            body: offer.sdp
        });

        if (isCancelled) return;

        if (response.status !== 201) {
            throw new Error(`WHEP POST request failed: ${response.status} ${response.statusText}`);
        }
        
        const location = response.headers.get('Location');
        if (!location) {
             throw new Error('WHEP response is missing a Location header');
        }
        sessionUrl = new URL(location, whepUrl).href;

        const answerSdp = await response.text();
        await pc.setRemoteDescription({ type: 'answer', sdp: answerSdp });

      } catch (e) {
        if (!isCancelled) {
            console.error(`WebRTC connection failed for ${streamPath}:`, e);
            setStatus('error');
        }
      }
    };

    connect();

    return () => {
      isCancelled = true;
      if (sessionUrl) {
          fetch(sessionUrl, { method: 'DELETE' }).catch(() => {});
      }
      pc.close();
    };
  }, [streamPath]);

  return (
    <div className="relative w-full h-full bg-black">
      <video
        ref={videoRef}
        autoPlay
        playsInline
        muted={isMuted}
        className={`w-full h-full object-cover transition-opacity duration-300 ${status === 'playing' ? 'opacity-100' : 'opacity-0'}`}
        onPlay={() => setStatus('playing')}
        onPlaying={() => setStatus('playing')}
      />
      {status === 'loading' && (
        <div className="absolute inset-0 flex items-center justify-center">
          <Loader2 className="w-8 h-8 text-slate-400 animate-spin" />
        </div>
      )}
      {status === 'error' && (
        <div className="absolute inset-0 flex flex-col items-center justify-center bg-slate-900/80 p-2 text-center">
          <VideoOff className="w-8 h-8 text-red-500 mb-2" />
          <p className="text-xs text-slate-400">Stream unavailable</p>
        </div>
      )}
    </div>
  );
};

export default WebRTCPlayer;