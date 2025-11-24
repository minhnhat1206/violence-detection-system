import React from 'react';

const Analytics: React.FC = () => {
  return (
    <div className="bg-slate-900/50 p-6 rounded-xl border border-slate-800 shadow-lg">
      <div className="h-[800px] w-full">
        <iframe
          src="http://localhost:3001/dashboard/snapshot/im9QlXQVi4JUXziOmQouAHtVNiMctokh"
          width="100%"
          height="100%"
          frameBorder="0"
        ></iframe>
      </div>
    </div>
  );
};

export default Analytics;
