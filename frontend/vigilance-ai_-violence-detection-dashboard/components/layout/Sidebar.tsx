
import React from 'react';
import { Shield } from 'lucide-react';
import { NAV_ITEMS } from '../../constants';

interface SidebarProps {
  activePage: string;
  setActivePage: (page: string) => void;
}

const Sidebar: React.FC<SidebarProps> = ({ activePage, setActivePage }) => {
  return (
    <aside className="w-64 bg-slate-900/70 backdrop-blur-sm border-r border-slate-800 flex flex-col">
      <div className="flex items-center justify-center h-20 border-b border-slate-800">
        <Shield className="h-8 w-8 text-emerald-500" />
        <h1 className="text-xl font-bold ml-3 text-slate-100">Vigilance AI</h1>
      </div>
      <nav className="flex-1 px-4 py-6">
        <ul>
          {NAV_ITEMS.map((item) => (
            <li key={item.name} className="mb-2">
              <button
                onClick={() => setActivePage(item.name)}
                className={`w-full flex items-center py-3 px-4 rounded-lg transition-all duration-200 ease-in-out
                  ${
                    activePage === item.name
                      ? 'bg-emerald-500/10 text-emerald-400 font-semibold'
                      : 'text-slate-400 hover:bg-slate-800/50 hover:text-slate-200'
                  }`}
              >
                <item.icon className="h-5 w-5 mr-4" />
                <span>{item.name}</span>
              </button>
            </li>
          ))}
        </ul>
      </nav>
      <div className="p-4 border-t border-slate-800">
          <p className="text-xs text-slate-500">© 2024 Vigilance Systems</p>
      </div>
    </aside>
  );
};

export default Sidebar;
