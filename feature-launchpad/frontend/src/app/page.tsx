'use client';

import { useEffect } from 'react';
import ProductConfigurator from '../components/ProductConfigurator';
import { initTracker } from '../lib/tracking';

export default function Home() {
  useEffect(() => {
    // Initialize tracker on mount
    const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
    
    initTracker({
      apiUrl,
      featureName: 'product_configurator',
      debug: process.env.NODE_ENV === 'development',
      batchSize: 5,
      flushInterval: 3000,
    });
  }, []);

  return (
    <main>
      <ProductConfigurator />
    </main>
  );
}
