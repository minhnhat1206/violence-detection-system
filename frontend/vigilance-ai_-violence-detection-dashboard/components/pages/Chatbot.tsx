import React, { useState, useRef, useEffect } from 'react';
import { Send, Bot, User, Loader2 } from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

type ChatMessage = { role: 'user' | 'model'; content: string };

const Chatbot: React.FC = () => {
  const [messages, setMessages] = useState<ChatMessage[]>([
    { role: 'model', content: "Xin chào! Tôi là trợ lý Vigilance AI. Bạn muốn truy vấn hệ thống như thế nào hôm nay?" }
  ]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement | null>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  useEffect(scrollToBottom, [messages]);

  const handleSendMessage = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || isLoading) return;

    const userMessage: ChatMessage = { role: 'user', content: input };
    setMessages(prev => [...prev, userMessage]);
    setInput('');
    setIsLoading(true);

    try {
      const response = await fetch('/rag/chat', {
        method: "POST",
        headers: {
          "Content-Type": "application/json; charset=utf-8"
        },
        body: JSON.stringify({ question: input })
      });

      if (!response.ok) {
        const text = await response.text();
        throw new Error(`HTTP ${response.status}: ${text}`);
      }

      const data = await response.json();
      const answer: string = data.answer || "Không có câu trả lời.";
      setMessages(prev => [...prev, { role: 'model', content: answer }]);
    } catch (err: any) {
      const msg = typeof err?.message === 'string' ? err.message : 'Lỗi không xác định';
      setMessages(prev => [...prev, { role: 'model', content: `⚠️ Lỗi khi gọi rag_chatbot: ${msg}` }]);
    } finally {
      setIsLoading(false);
    }
  };

  const renderMarkdown = (text: string) => (
    <ReactMarkdown
      remarkPlugins={[remarkGfm]}
      components={{
        strong: ({node, ...props}) => (
          <span
            {...props}
            className={
              text.includes('normal')
                ? 'text-green-400 font-bold'
                : text.includes('violence')
                ? 'text-red-500 font-bold'
                : 'font-bold'
            }
          />
        )
      }}
    >
      {text}
    </ReactMarkdown>
  );

  return (
    <div className="flex flex-col h-[calc(100vh-10rem)] max-w-4xl mx-auto bg-slate-900/50 rounded-xl border border-slate-800">
      <div className="flex-1 p-6 overflow-y-auto">
        <div className="space-y-6">
          {messages.map((message, index) => (
            <div key={index} className={`flex items-start gap-4 ${message.role === 'user' ? 'justify-end' : ''}`}>
              {message.role === 'model' && (
                <div className="w-10 h-10 rounded-full bg-emerald-500/20 flex items-center justify-center flex-shrink-0">
                  <Bot className="w-6 h-6 text-emerald-400" />
                </div>
              )}
              <div className={`px-4 py-3 rounded-xl max-w-lg ${
                message.role === 'user' 
                  ? 'bg-blue-600/50 text-white rounded-br-none' 
                  : 'bg-slate-800 text-slate-300 rounded-bl-none'
              }`}>
                <div className="text-sm leading-relaxed">
                  {renderMarkdown(message.content)}
                </div>
              </div>
              {message.role === 'user' && (
                <div className="w-10 h-10 rounded-full bg-slate-700 flex items-center justify-center flex-shrink-0">
                  <User className="w-6 h-6 text-slate-300" />
                </div>
              )}
            </div>
          ))}
          {isLoading && (
            <div className="flex items-start gap-4">
              <div className="w-10 h-10 rounded-full bg-emerald-500/20 flex items-center justify-center flex-shrink-0">
                <Bot className="w-6 h-6 text-emerald-400" />
              </div>
              <div className="px-4 py-3 rounded-xl max-w-lg bg-slate-800 text-slate-300 rounded-bl-none">
                <Loader2 className="w-5 h-5 animate-spin" />
              </div>
            </div>
          )}
          <div ref={messagesEndRef} />
        </div>
      </div>
      <div className="p-4 border-t border-slate-800">
        <form onSubmit={handleSendMessage} className="flex items-center gap-4">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Hỏi về alert, camera, hoặc xu hướng..."
            className="flex-1 bg-slate-800 border border-slate-700 rounded-lg py-3 px-4 focus:outline-none focus:ring-2 focus:ring-emerald-500 transition-all"
            disabled={isLoading}
          />
          <button
            type="submit"
            className="bg-emerald-600 hover:bg-emerald-500 text-white font-bold py-3 px-5 rounded-lg transition-all disabled:bg-slate-600 disabled:cursor-not-allowed flex items-center"
            disabled={isLoading || !input.trim()}
          >
            <Send className="w-5 h-5" />
          </button>
        </form>
      </div>
    </div>
  );
};

export default Chatbot;
