
import React, { useState } from 'react';
import Sidebar from './components/layout/Sidebar';
import Header from './components/layout/Header';
import LiveStreams from './components/pages/LiveStreams';
import AlertsDashboard from './components/pages/AlertsDashboard';
import Analytics from './components/pages/Analytics';
import Chatbot from './components/pages/Chatbot';
import Settings from './components/pages/Settings';
import { NAV_ITEMS } from './constants';

const App: React.FC = () => {
  const [activePage, setActivePage] = useState<string>(NAV_ITEMS[0].name);

  const renderContent = () => {
    switch (activePage) {
      case 'Live Streams':
        return <LiveStreams />;
      case 'Alerts Dashboard':
        return <AlertsDashboard />;
      case 'Analytics':
        return <Analytics />;
      case 'Chatbot':
        return <Chatbot />;
      case 'Settings':
        return <Settings />;
      default:
        return <LiveStreams />;
    }
  };

  return (
    <div className="flex h-screen bg-slate-950 text-slate-300">
      <Sidebar activePage={activePage} setActivePage={setActivePage} />
      <div className="flex flex-col flex-1 overflow-hidden">
        <Header currentPage={activePage} />
        <main className="flex-1 overflow-y-auto p-4 md:p-6 lg:p-8">
          {renderContent()}
        </main>
      </div>
    </div>
  );
};

export default App;
