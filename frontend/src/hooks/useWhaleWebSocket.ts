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

    // Fetch initial history
    const httpUrl = url.replace('ws://', 'http://').replace('wss://', 'https://').replace('/ws/whales', '/api/v1/whales');
    fetch(httpUrl)
      .then(res => res.json())
      .then(data => {
        if (!mountedRef.current) return;
        if (data && Array.isArray(data.whales)) {
          const fetchedWhales = data.whales.map((w: any) => ({
            id: w.hash || w.tx_hash,
            tx_hash: w.hash || w.tx_hash,
            from_address: w.from_address,
            to_address: w.to_address,
            value: w.value,
            value_eth: w.value_eth,
            from_label: w.from_label,
            to_label: w.to_label,
            block_number: w.block_number,
            timestamp: w.timestamp
          }));
          
          setMessages(prev => {
            const merged = [...prev];
            for (const whale of fetchedWhales) {
              if (!merged.some(p => p.id === whale.id)) {
                merged.push(whale);
              }
            }
            // Sort by timestamp descending
            merged.sort((a, b) => new Date(b.timestamp || 0).getTime() - new Date(a.timestamp || 0).getTime());
            return merged.slice(0, MAX_ALERTS);
          });
        }
      })
      .catch(console.error);

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

          setMessages((prev) => {
            // Check for duplicates
            if (prev.some((p) => p.id === alert.id)) return prev;
            return [alert, ...prev].slice(0, MAX_ALERTS);
          });
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