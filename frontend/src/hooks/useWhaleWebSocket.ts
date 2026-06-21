import { useEffect, useRef, useState, useCallback } from 'react';

export type ConnectionStatus = 'live' | 'reconnecting' | 'offline';

export interface WhaleAlert {
  id: string;
  tx_hash: string;
  from_address: string;
  to_address: string;
  value: number;
  value_eth: number;
  from_label?: string;
  to_label?: string;
  block_number?: number;
  timestamp?: string;
}

const MAX_ALERTS = 50;
const MAX_RECONNECT_DELAY = 16_000;

export function useWhaleWebSocket(url: string) {
  const [messages, setMessages] = useState<WhaleAlert[]>([]);
  const [status, setStatus] = useState<ConnectionStatus>('offline');
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectAttempt = useRef(0);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const mountedRef = useRef(true);

  const connect = useCallback(() => {
    if (!mountedRef.current) return;

    try {
      const ws = new WebSocket(url);
      wsRef.current = ws;
      setStatus('reconnecting');

      ws.onopen = () => {
        if (!mountedRef.current) return;
        setStatus('live');
        reconnectAttempt.current = 0;
      };

      ws.onmessage = (event) => {
        if (!mountedRef.current) return;
        try {
          const data = JSON.parse(event.data);

          // Ignore heartbeat pings
          if (data.type === 'ping') return;

          const alert: WhaleAlert = {
            id: data.tx_hash || `${Date.now()}-${Math.random()}`,
            tx_hash: data.tx_hash || '',
            from_address: data.from_address || '',
            to_address: data.to_address || '',
            value: data.value || 0,
            value_eth: data.value_eth || 0,
            from_label: data.from_label,
            to_label: data.to_label,
            block_number: data.block_number,
            timestamp: data.timestamp || new Date().toISOString(),
          };

          setMessages((prev) => [alert, ...prev].slice(0, MAX_ALERTS));
        } catch {
          // Ignore malformed messages
        }
      };

      ws.onclose = () => {
        if (!mountedRef.current) return;
        scheduleReconnect();
      };

      ws.onerror = () => {
        if (!mountedRef.current) return;
        ws.close();
      };
    } catch {
      scheduleReconnect();
    }
  }, [url]);

  const scheduleReconnect = useCallback(() => {
    if (!mountedRef.current) return;
    setStatus('reconnecting');

    const attempt = reconnectAttempt.current;
    const delay = Math.min(1000 * 2 ** attempt, MAX_RECONNECT_DELAY);
    reconnectAttempt.current = attempt + 1;

    // After 5 failed attempts, mark as offline
    if (attempt >= 5) {
      setStatus('offline');
    }

    reconnectTimer.current = setTimeout(() => {
      if (mountedRef.current) connect();
    }, delay);
  }, [connect]);

  useEffect(() => {
    mountedRef.current = true;
    connect();

    return () => {
      mountedRef.current = false;
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
      if (wsRef.current) {
        wsRef.current.onclose = null;
        wsRef.current.close();
      }
    };
  }, [connect]);

  return { messages, status };
}