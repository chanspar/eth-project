import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import Header from './components/Header';
import Dashboard from './pages/Dashboard';
import TokenDirectoryPage from './pages/TokenDirectoryPage';
import { useWhaleWebSocket } from './hooks/useWhaleWebSocket';

const WS_URL = 'ws://localhost:8000/ws/whales';

function App() {
  const { messages, status } = useWhaleWebSocket(WS_URL);

  return (
    <Router>
      <Header status={status} />
      <Routes>
        <Route path="/" element={<Dashboard messages={messages} />} />
        <Route path="/directory" element={<TokenDirectoryPage />} />
      </Routes>
    </Router>
  );
}

export default App;
