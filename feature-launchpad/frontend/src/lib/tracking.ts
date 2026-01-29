/**
 * Feature Launchpad Event Tracking SDK
 * 
 * A lightweight, type-safe event tracking library for capturing
 * user interactions and sending them to the backend API.
 * 
 * Features:
 * - Automatic session management
 * - Event batching for efficiency
 * - Retry logic with exponential backoff
 * - Offline event queue
 * - TypeScript support
 */

// Types
export type EventType =
  | 'page_view'
  | 'feature_view'
  | 'question_viewed'
  | 'slider_adjusted'
  | 'option_selected'
  | 'option_deselected'
  | 'quiz_started'
  | 'quiz_step_completed'
  | 'quiz_completed'
  | 'quiz_abandoned'
  | 'result_viewed'
  | 'result_shared'
  | 'result_saved'
  | 'cta_clicked'
  | 'link_clicked'
  | 'session_start'
  | 'session_end';

export interface EventProperties {
  question_id?: number;
  step_number?: number;
  total_steps?: number;
  slider_value?: number;
  previous_value?: number;
  option_id?: string;
  option_label?: string;
  selected_options?: string[];
  recommendation_id?: string;
  recommendation_name?: string;
  match_score?: number;
  time_on_step_ms?: number;
  total_time_ms?: number;
  share_platform?: string;
  cta_id?: string;
  cta_label?: string;
  extra?: Record<string, unknown>;
}

export interface EventContext {
  page_url?: string;
  page_path?: string;
  referrer?: string;
  user_agent?: string;
  device_type?: 'desktop' | 'mobile' | 'tablet' | 'unknown';
  screen_width?: number;
  screen_height?: number;
  viewport_width?: number;
  viewport_height?: number;
  timezone?: string;
  locale?: string;
}

export interface TrackingEvent {
  event_id: string;
  user_id: string;
  session_id: string;
  event_type: EventType;
  event_properties: EventProperties;
  feature_name: string;
  context: EventContext;
  timestamp: string;
}

export interface TrackerConfig {
  apiUrl: string;
  featureName?: string;
  batchSize?: number;
  flushInterval?: number;
  maxRetries?: number;
  debug?: boolean;
}

// Utility functions
const generateUUID = (): string => {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
};

const getDeviceType = (): 'desktop' | 'mobile' | 'tablet' | 'unknown' => {
  if (typeof window === 'undefined') return 'unknown';
  
  const ua = navigator.userAgent.toLowerCase();
  if (/(tablet|ipad|playbook|silk)|(android(?!.*mobi))/i.test(ua)) {
    return 'tablet';
  }
  if (/mobile|android|iphone|ipod|blackberry|opera mini|iemobile/i.test(ua)) {
    return 'mobile';
  }
  return 'desktop';
};

const getStorageItem = (key: string): string | null => {
  if (typeof window === 'undefined') return null;
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
};

const setStorageItem = (key: string, value: string): void => {
  if (typeof window === 'undefined') return;
  try {
    localStorage.setItem(key, value);
  } catch {
    // Ignore storage errors
  }
};

/**
 * Event Tracker Class
 * 
 * Main tracking interface for capturing and sending events.
 */
class EventTracker {
  private config: Required<TrackerConfig>;
  private userId: string;
  private sessionId: string;
  private eventQueue: TrackingEvent[] = [];
  private flushTimer: ReturnType<typeof setInterval> | null = null;
  private sessionStartTime: number;
  private stepStartTime: number;
  private isInitialized = false;

  constructor(config: TrackerConfig) {
    this.config = {
      apiUrl: config.apiUrl,
      featureName: config.featureName || 'product_configurator',
      batchSize: config.batchSize || 10,
      flushInterval: config.flushInterval || 5000,
      maxRetries: config.maxRetries || 3,
      debug: config.debug || false,
    };

    // Initialize user and session
    this.userId = this.getOrCreateUserId();
    this.sessionId = this.getOrCreateSessionId();
    this.sessionStartTime = Date.now();
    this.stepStartTime = Date.now();

    this.log('Tracker initialized', {
      userId: this.userId,
      sessionId: this.sessionId,
    });
  }

  /**
   * Initialize the tracker and start automatic flushing
   */
  public init(): void {
    if (this.isInitialized) return;

    // Start flush interval
    this.flushTimer = setInterval(() => {
      this.flush();
    }, this.config.flushInterval);

    // Track session start
    this.track('session_start');

    // Handle page unload
    if (typeof window !== 'undefined') {
      window.addEventListener('beforeunload', () => {
        this.track('session_end', {
          total_time_ms: Date.now() - this.sessionStartTime,
        });
        this.flush(true); // Synchronous flush
      });

      // Handle visibility change (tab switching)
      document.addEventListener('visibilitychange', () => {
        if (document.visibilityState === 'hidden') {
          this.flush();
        }
      });
    }

    this.isInitialized = true;
    this.log('Tracker started');
  }

  /**
   * Track an event
   */
  public track(
    eventType: EventType,
    properties: EventProperties = {},
    context: Partial<EventContext> = {}
  ): void {
    const event: TrackingEvent = {
      event_id: generateUUID(),
      user_id: this.userId,
      session_id: this.sessionId,
      event_type: eventType,
      event_properties: properties,
      feature_name: this.config.featureName,
      context: this.buildContext(context),
      timestamp: new Date().toISOString(),
    };

    this.eventQueue.push(event);
    this.log('Event tracked', event);

    // Auto-flush if queue is full
    if (this.eventQueue.length >= this.config.batchSize) {
      this.flush();
    }
  }

  /**
   * Track quiz start
   */
  public trackQuizStart(totalSteps: number): void {
    this.sessionStartTime = Date.now();
    this.stepStartTime = Date.now();
    this.track('quiz_started', {
      total_steps: totalSteps,
    });
  }

  /**
   * Track question view
   */
  public trackQuestionViewed(questionId: number, stepNumber: number, totalSteps: number): void {
    const timeOnPrevStep = Date.now() - this.stepStartTime;
    this.stepStartTime = Date.now();

    this.track('question_viewed', {
      question_id: questionId,
      step_number: stepNumber,
      total_steps: totalSteps,
      time_on_step_ms: timeOnPrevStep,
    });
  }

  /**
   * Track slider adjustment
   */
  public trackSliderAdjusted(
    questionId: number,
    value: number,
    previousValue: number
  ): void {
    this.track('slider_adjusted', {
      question_id: questionId,
      slider_value: value,
      previous_value: previousValue,
    });
  }

  /**
   * Track option selection
   */
  public trackOptionSelected(
    questionId: number,
    optionId: string,
    optionLabel: string
  ): void {
    this.track('option_selected', {
      question_id: questionId,
      option_id: optionId,
      option_label: optionLabel,
    });
  }

  /**
   * Track step completion
   */
  public trackStepCompleted(
    stepNumber: number,
    totalSteps: number,
    selectedOptions?: string[]
  ): void {
    const timeOnStep = Date.now() - this.stepStartTime;
    this.stepStartTime = Date.now();

    this.track('quiz_step_completed', {
      step_number: stepNumber,
      total_steps: totalSteps,
      time_on_step_ms: timeOnStep,
      selected_options: selectedOptions,
    });
  }

  /**
   * Track quiz completion
   */
  public trackQuizCompleted(
    recommendationId: string,
    recommendationName: string,
    matchScore: number
  ): void {
    const totalTime = Date.now() - this.sessionStartTime;

    this.track('quiz_completed', {
      recommendation_id: recommendationId,
      recommendation_name: recommendationName,
      match_score: matchScore,
      total_time_ms: totalTime,
    });
  }

  /**
   * Track result share
   */
  public trackResultShared(platform: string, recommendationId: string): void {
    this.track('result_shared', {
      share_platform: platform,
      recommendation_id: recommendationId,
    });
  }

  /**
   * Track CTA click
   */
  public trackCtaClicked(ctaId: string, ctaLabel: string): void {
    this.track('cta_clicked', {
      cta_id: ctaId,
      cta_label: ctaLabel,
    });
  }

  /**
   * Flush events to the server
   */
  public async flush(sync = false): Promise<void> {
    if (this.eventQueue.length === 0) return;

    const events = [...this.eventQueue];
    this.eventQueue = [];

    if (sync && typeof navigator !== 'undefined' && navigator.sendBeacon) {
      // Use sendBeacon for synchronous flush (page unload)
      const url = `${this.config.apiUrl}/api/v1/events/track/batch`;
      const blob = new Blob([JSON.stringify({ events })], {
        type: 'application/json',
      });
      navigator.sendBeacon(url, blob);
      this.log('Events flushed via beacon', { count: events.length });
    } else {
      // Use fetch for async flush
      await this.sendEvents(events);
    }
  }

  /**
   * Send events to the API with retry logic
   */
  private async sendEvents(events: TrackingEvent[], attempt = 1): Promise<void> {
    const url = `${this.config.apiUrl}/api/v1/events/track/batch`;

    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ events }),
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      this.log('Events sent successfully', { count: events.length });
    } catch (error) {
      this.log('Failed to send events', { error, attempt });

      if (attempt < this.config.maxRetries) {
        // Exponential backoff
        const delay = Math.pow(2, attempt) * 1000;
        await new Promise((resolve) => setTimeout(resolve, delay));
        return this.sendEvents(events, attempt + 1);
      }

      // Re-queue events on final failure
      this.eventQueue = [...events, ...this.eventQueue];
      this.log('Events re-queued after max retries');
    }
  }

  /**
   * Build event context
   */
  private buildContext(override: Partial<EventContext>): EventContext {
    if (typeof window === 'undefined') {
      return override as EventContext;
    }

    return {
      page_url: window.location.href,
      page_path: window.location.pathname,
      referrer: document.referrer || undefined,
      user_agent: navigator.userAgent,
      device_type: getDeviceType(),
      screen_width: window.screen.width,
      screen_height: window.screen.height,
      viewport_width: window.innerWidth,
      viewport_height: window.innerHeight,
      timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
      locale: navigator.language,
      ...override,
    };
  }

  /**
   * Get or create anonymous user ID
   */
  private getOrCreateUserId(): string {
    const key = 'fl_user_id';
    let userId = getStorageItem(key);

    if (!userId) {
      userId = `anon_${generateUUID().replace(/-/g, '').substring(0, 16)}`;
      setStorageItem(key, userId);
    }

    return userId;
  }

  /**
   * Get or create session ID
   */
  private getOrCreateSessionId(): string {
    const key = 'fl_session_id';
    const timeKey = 'fl_session_time';
    const SESSION_TIMEOUT = 30 * 60 * 1000; // 30 minutes

    const existingSession = getStorageItem(key);
    const lastActivity = getStorageItem(timeKey);

    // Check if session is still valid
    if (existingSession && lastActivity) {
      const timeSinceLastActivity = Date.now() - parseInt(lastActivity, 10);
      if (timeSinceLastActivity < SESSION_TIMEOUT) {
        setStorageItem(timeKey, Date.now().toString());
        return existingSession;
      }
    }

    // Create new session
    const sessionId = generateUUID();
    setStorageItem(key, sessionId);
    setStorageItem(timeKey, Date.now().toString());
    return sessionId;
  }

  /**
   * Debug logging
   */
  private log(message: string, data?: unknown): void {
    if (this.config.debug) {
      console.log(`[FeatureLaunchpad] ${message}`, data || '');
    }
  }

  /**
   * Clean up resources
   */
  public destroy(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
    }
    this.flush(true);
  }
}

// Singleton instance for easy use
let trackerInstance: EventTracker | null = null;

/**
 * Initialize the tracker (call once at app startup)
 */
export const initTracker = (config: TrackerConfig): EventTracker => {
  if (trackerInstance) {
    return trackerInstance;
  }

  trackerInstance = new EventTracker(config);
  trackerInstance.init();
  return trackerInstance;
};

/**
 * Get the tracker instance
 */
export const getTracker = (): EventTracker | null => {
  return trackerInstance;
};

/**
 * Convenience function to track events
 */
export const track = (
  eventType: EventType,
  properties?: EventProperties,
  context?: Partial<EventContext>
): void => {
  if (trackerInstance) {
    trackerInstance.track(eventType, properties, context);
  } else {
    console.warn('[FeatureLaunchpad] Tracker not initialized. Call initTracker() first.');
  }
};

export default EventTracker;
