import { GoogleGenAI, Chat } from "@google/genai";
import { ChatMessage } from '../types';

const API_KEY = process.env.API_KEY;

if (!API_KEY) {
  console.warn("API_KEY environment variable not set. Chatbot will not function.");
}

const ai = new GoogleGenAI({ apiKey: API_KEY! });

const model = ai.chats.create({
  model: 'gemini-2.5-flash',
  config: {
    systemInstruction: `You are an AI assistant for the Vigilance AI security dashboard. 
    You can answer questions about security alerts, camera statuses, and analytical trends based on the data available in the system.
    Be concise and helpful. When asked about data, provide a summary. 
    You can query information about:
    - Live camera streams and their status (Normal, Violence Detected, Offline).
    - Security alerts, including location, timestamp, and violence score.
    - Analytics like alert trends over time or top locations with incidents.
    - Available locations include: Quận 1, Quận 3, Quận 5, Quận 7, Quận 10, Quận Gò Vấp, Quận Bình Thạnh, Quận Tân Bình, and Thành phố Thủ Đức.
    - Alert types are: Fight, Crowd, Anomaly.`,
  },
});

export const runChat = async (history: ChatMessage[], newMessage: string): Promise<string> => {
  if (!API_KEY) {
    return "Chatbot is disabled. Please configure the API key.";
  }
  
  try {
    const result = await model.sendMessage({ message: newMessage });
    return result.text;
  } catch (error) {
    console.error("Gemini API error:", error);
    return "Sorry, I encountered an error. Please try again later.";
  }
};
