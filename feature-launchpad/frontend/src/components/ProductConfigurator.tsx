'use client';

import React, { useState, useEffect, useCallback } from 'react';
import { getTracker } from '../lib/tracking';

// Types
interface Question {
  id: number;
  title: string;
  subtitle: string;
  type: 'slider' | 'single' | 'multi';
  options?: Option[];
  sliderConfig?: {
    min: number;
    max: number;
    step: number;
    labels: { value: number; label: string }[];
  };
}

interface Option {
  id: string;
  label: string;
  icon: string;
  description?: string;
}

interface Recommendation {
  id: string;
  name: string;
  category: string;
  description: string;
  matchScore: number;
  features: string[];
  icon: string;
  color: string;
}

// Questions data
const questions: Question[] = [
  {
    id: 1,
    title: 'What\'s the size of your team?',
    subtitle: 'This helps us recommend tools that scale appropriately',
    type: 'slider',
    sliderConfig: {
      min: 1,
      max: 100,
      step: 1,
      labels: [
        { value: 1, label: 'Solo' },
        { value: 10, label: 'Small' },
        { value: 50, label: 'Medium' },
        { value: 100, label: 'Enterprise' },
      ],
    },
  },
  {
    id: 2,
    title: 'What\'s your primary use case?',
    subtitle: 'Select the category that best describes your needs',
    type: 'single',
    options: [
      { id: 'analytics', label: 'Data Analytics', icon: '📊', description: 'BI, reporting, dashboards' },
      { id: 'engineering', label: 'Data Engineering', icon: '⚙️', description: 'Pipelines, ETL, orchestration' },
      { id: 'ml', label: 'Machine Learning', icon: '🤖', description: 'Training, deployment, MLOps' },
      { id: 'realtime', label: 'Real-time Processing', icon: '⚡', description: 'Streaming, events, CDC' },
    ],
  },
  {
    id: 3,
    title: 'Which features matter most?',
    subtitle: 'Select all that apply to your requirements',
    type: 'multi',
    options: [
      { id: 'opensource', label: 'Open Source', icon: '🔓' },
      { id: 'cloud', label: 'Cloud Native', icon: '☁️' },
      { id: 'sql', label: 'SQL Support', icon: '💾' },
      { id: 'python', label: 'Python SDK', icon: '🐍' },
      { id: 'governance', label: 'Data Governance', icon: '🛡️' },
      { id: 'lowcode', label: 'Low-Code', icon: '🎨' },
    ],
  },
  {
    id: 4,
    title: 'What\'s your budget range?',
    subtitle: 'Monthly spend per user (USD)',
    type: 'slider',
    sliderConfig: {
      min: 0,
      max: 500,
      step: 10,
      labels: [
        { value: 0, label: 'Free' },
        { value: 100, label: '$100' },
        { value: 250, label: '$250' },
        { value: 500, label: '$500+' },
      ],
    },
  },
];

// Recommendations database
const recommendations: Record<string, Recommendation> = {
  databricks: {
    id: 'databricks',
    name: 'Databricks',
    category: 'Unified Analytics Platform',
    description: 'Best for teams needing end-to-end data engineering and ML capabilities with collaborative notebooks.',
    matchScore: 0,
    features: ['Spark-based processing', 'MLflow integration', 'Delta Lake', 'Collaborative notebooks'],
    icon: '🔷',
    color: '#FF3621',
  },
  snowflake: {
    id: 'snowflake',
    name: 'Snowflake',
    category: 'Cloud Data Warehouse',
    description: 'Ideal for SQL-heavy analytics workloads with seamless scaling and data sharing.',
    matchScore: 0,
    features: ['Zero-copy cloning', 'Time travel', 'Data sharing', 'Elastic compute'],
    icon: '❄️',
    color: '#29B5E8',
  },
  duckdb: {
    id: 'duckdb',
    name: 'DuckDB',
    category: 'Embedded Analytics',
    description: 'Perfect for solo developers or small teams needing fast, local analytics.',
    matchScore: 0,
    features: ['Zero dependencies', 'Blazing fast', 'SQL support', 'Free & open source'],
    icon: '🦆',
    color: '#FFC107',
  },
  kafka: {
    id: 'kafka',
    name: 'Apache Kafka',
    category: 'Event Streaming',
    description: 'The industry standard for high-throughput real-time data streaming.',
    matchScore: 0,
    features: ['High throughput', 'Fault tolerant', 'Scalable', 'Stream processing'],
    icon: '📨',
    color: '#231F20',
  },
  airflow: {
    id: 'airflow',
    name: 'Apache Airflow',
    category: 'Workflow Orchestration',
    description: 'Powerful workflow automation for complex data pipelines.',
    matchScore: 0,
    features: ['DAG-based workflows', 'Extensive operators', 'Python native', 'Rich UI'],
    icon: '🌀',
    color: '#017CEE',
  },
  metabase: {
    id: 'metabase',
    name: 'Metabase',
    category: 'Business Intelligence',
    description: 'User-friendly BI tool that makes data accessible to everyone.',
    matchScore: 0,
    features: ['No-code queries', 'Beautiful dashboards', 'Embedded analytics', 'Open source'],
    icon: '📈',
    color: '#509EE3',
  },
};

// Calculate recommendation based on answers
const calculateRecommendation = (answers: Record<number, number | string | string[]>): Recommendation => {
  const scores: Record<string, number> = {};
  Object.keys(recommendations).forEach((key) => {
    scores[key] = 50; // Base score
  });

  // Team size scoring
  const teamSize = answers[1] as number || 10;
  if (teamSize <= 5) {
    scores.duckdb += 30;
    scores.metabase += 20;
  } else if (teamSize <= 25) {
    scores.airflow += 20;
    scores.metabase += 25;
  } else {
    scores.databricks += 25;
    scores.snowflake += 30;
    scores.kafka += 20;
  }

  // Use case scoring
  const useCase = answers[2] as string;
  if (useCase === 'analytics') {
    scores.snowflake += 35;
    scores.metabase += 30;
    scores.duckdb += 20;
  } else if (useCase === 'engineering') {
    scores.airflow += 35;
    scores.databricks += 25;
  } else if (useCase === 'ml') {
    scores.databricks += 40;
  } else if (useCase === 'realtime') {
    scores.kafka += 45;
  }

  // Features scoring
  const features = answers[3] as string[] || [];
  features.forEach((feature) => {
    if (feature === 'opensource') {
      scores.duckdb += 15;
      scores.airflow += 15;
      scores.kafka += 15;
      scores.metabase += 15;
    }
    if (feature === 'cloud') {
      scores.snowflake += 15;
      scores.databricks += 15;
    }
    if (feature === 'sql') {
      scores.snowflake += 20;
      scores.duckdb += 20;
    }
    if (feature === 'python') {
      scores.airflow += 15;
      scores.databricks += 15;
    }
    if (feature === 'lowcode') {
      scores.metabase += 25;
    }
  });

  // Budget scoring
  const budget = answers[4] as number || 100;
  if (budget <= 50) {
    scores.duckdb += 30;
    scores.metabase += 20;
  } else if (budget >= 300) {
    scores.databricks += 20;
    scores.snowflake += 25;
  }

  // Find best match
  let bestMatch = 'duckdb';
  let highestScore = 0;
  Object.entries(scores).forEach(([key, score]) => {
    if (score > highestScore) {
      highestScore = score;
      bestMatch = key;
    }
  });

  // Normalize to percentage
  const maxPossibleScore = 200;
  const matchScore = Math.min(99, Math.round((highestScore / maxPossibleScore) * 100));

  return {
    ...recommendations[bestMatch],
    matchScore,
  };
};

// Progress indicator component
const ProgressIndicator: React.FC<{ current: number; total: number }> = ({ current, total }) => (
  <div className="flex items-center gap-2 mb-8">
    {Array.from({ length: total }).map((_, i) => (
      <div
        key={i}
        className={`h-1.5 flex-1 rounded-full transition-all duration-500 ${
          i < current ? 'bg-indigo-500' : i === current ? 'bg-indigo-300' : 'bg-gray-200'
        }`}
      />
    ))}
  </div>
);

// Slider question component
const SliderQuestion: React.FC<{
  question: Question;
  value: number;
  onChange: (value: number) => void;
}> = ({ question, value, onChange }) => {
  const config = question.sliderConfig!;
  const [localValue, setLocalValue] = useState(value);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const newValue = parseInt(e.target.value);
    setLocalValue(newValue);
  };

  const handleMouseUp = () => {
    if (localValue !== value) {
      onChange(localValue);
    }
  };

  const percentage = ((localValue - config.min) / (config.max - config.min)) * 100;

  return (
    <div className="space-y-6">
      <div className="relative pt-8 pb-4">
        <div
          className="absolute -top-2 transform -translate-x-1/2 bg-indigo-600 text-white px-3 py-1 rounded-lg text-sm font-medium shadow-lg transition-all duration-150"
          style={{ left: `${percentage}%` }}
        >
          {localValue}
          <div className="absolute bottom-0 left-1/2 transform -translate-x-1/2 translate-y-full">
            <div className="border-8 border-transparent border-t-indigo-600" />
          </div>
        </div>
        <input
          type="range"
          min={config.min}
          max={config.max}
          step={config.step}
          value={localValue}
          onChange={handleChange}
          onMouseUp={handleMouseUp}
          onTouchEnd={handleMouseUp}
          className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer accent-indigo-600"
          style={{
            background: `linear-gradient(to right, #4f46e5 0%, #4f46e5 ${percentage}%, #e5e7eb ${percentage}%, #e5e7eb 100%)`,
          }}
        />
      </div>
      <div className="flex justify-between text-sm text-gray-500">
        {config.labels.map((label) => (
          <span key={label.value} className="text-center">
            {label.label}
          </span>
        ))}
      </div>
    </div>
  );
};

// Single select component
const SingleSelect: React.FC<{
  options: Option[];
  selected: string;
  onChange: (id: string) => void;
}> = ({ options, selected, onChange }) => (
  <div className="grid grid-cols-2 gap-4">
    {options.map((option) => (
      <button
        key={option.id}
        onClick={() => onChange(option.id)}
        className={`p-4 rounded-xl border-2 text-left transition-all duration-200 ${
          selected === option.id
            ? 'border-indigo-500 bg-indigo-50 shadow-md'
            : 'border-gray-200 hover:border-indigo-200 hover:bg-gray-50'
        }`}
      >
        <div className="text-2xl mb-2">{option.icon}</div>
        <div className="font-medium text-gray-900">{option.label}</div>
        {option.description && (
          <div className="text-sm text-gray-500 mt-1">{option.description}</div>
        )}
      </button>
    ))}
  </div>
);

// Multi select component
const MultiSelect: React.FC<{
  options: Option[];
  selected: string[];
  onChange: (ids: string[]) => void;
}> = ({ options, selected, onChange }) => {
  const toggle = (id: string) => {
    if (selected.includes(id)) {
      onChange(selected.filter((s) => s !== id));
    } else {
      onChange([...selected, id]);
    }
  };

  return (
    <div className="grid grid-cols-3 gap-3">
      {options.map((option) => (
        <button
          key={option.id}
          onClick={() => toggle(option.id)}
          className={`p-3 rounded-xl border-2 text-center transition-all duration-200 ${
            selected.includes(option.id)
              ? 'border-indigo-500 bg-indigo-50 shadow-md'
              : 'border-gray-200 hover:border-indigo-200 hover:bg-gray-50'
          }`}
        >
          <div className="text-2xl mb-1">{option.icon}</div>
          <div className="text-sm font-medium text-gray-900">{option.label}</div>
        </button>
      ))}
    </div>
  );
};

// Result card component
const ResultCard: React.FC<{
  recommendation: Recommendation;
  onShare: (platform: string) => void;
  onRestart: () => void;
}> = ({ recommendation, onShare, onRestart }) => (
  <div className="text-center">
    <div className="mb-6">
      <div className="inline-flex items-center justify-center w-20 h-20 rounded-2xl text-4xl mb-4"
           style={{ backgroundColor: recommendation.color + '20' }}>
        {recommendation.icon}
      </div>
      <div className="flex items-center justify-center gap-2 mb-2">
        <span className="text-5xl font-bold text-gray-900">{recommendation.matchScore}%</span>
        <span className="text-gray-500">match</span>
      </div>
      <h2 className="text-2xl font-bold text-gray-900">{recommendation.name}</h2>
      <p className="text-indigo-600 font-medium">{recommendation.category}</p>
    </div>

    <p className="text-gray-600 mb-6 max-w-md mx-auto">{recommendation.description}</p>

    <div className="flex flex-wrap justify-center gap-2 mb-8">
      {recommendation.features.map((feature) => (
        <span
          key={feature}
          className="px-3 py-1 bg-gray-100 text-gray-700 rounded-full text-sm"
        >
          {feature}
        </span>
      ))}
    </div>

    <div className="flex flex-col sm:flex-row gap-3 justify-center">
      <button
        onClick={() => onShare('twitter')}
        className="px-6 py-3 bg-indigo-600 text-white rounded-xl font-medium hover:bg-indigo-700 transition-colors"
      >
        Share Result
      </button>
      <button
        onClick={onRestart}
        className="px-6 py-3 bg-gray-100 text-gray-700 rounded-xl font-medium hover:bg-gray-200 transition-colors"
      >
        Start Over
      </button>
    </div>
  </div>
);

// Main Configurator Component
export default function ProductConfigurator() {
  const [currentStep, setCurrentStep] = useState(0);
  const [answers, setAnswers] = useState<Record<number, number | string | string[]>>({
    1: 10,
    2: '',
    3: [],
    4: 100,
  });
  const [recommendation, setRecommendation] = useState<Recommendation | null>(null);
  const [isAnimating, setIsAnimating] = useState(false);

  const tracker = getTracker();

  // Track quiz start
  useEffect(() => {
    tracker?.trackQuizStart(questions.length);
  }, []);

  // Track question view
  useEffect(() => {
    if (currentStep < questions.length) {
      tracker?.trackQuestionViewed(questions[currentStep].id, currentStep + 1, questions.length);
    }
  }, [currentStep]);

  const handleSliderChange = useCallback((questionId: number, value: number) => {
    const prevValue = answers[questionId] as number;
    setAnswers((prev) => ({ ...prev, [questionId]: value }));
    tracker?.trackSliderAdjusted(questionId, value, prevValue);
  }, [answers]);

  const handleSingleSelect = useCallback((questionId: number, optionId: string) => {
    setAnswers((prev) => ({ ...prev, [questionId]: optionId }));
    const option = questions.find(q => q.id === questionId)?.options?.find(o => o.id === optionId);
    tracker?.trackOptionSelected(questionId, optionId, option?.label || '');
  }, []);

  const handleMultiSelect = useCallback((questionId: number, optionIds: string[]) => {
    setAnswers((prev) => ({ ...prev, [questionId]: optionIds }));
    // Track the latest selection
    const latestId = optionIds[optionIds.length - 1];
    if (latestId) {
      const option = questions.find(q => q.id === questionId)?.options?.find(o => o.id === latestId);
      tracker?.trackOptionSelected(questionId, latestId, option?.label || '');
    }
  }, []);

  const goNext = () => {
    setIsAnimating(true);
    
    // Track step completion
    const currentQuestion = questions[currentStep];
    tracker?.trackStepCompleted(
      currentStep + 1,
      questions.length,
      currentQuestion.type === 'multi' ? answers[currentQuestion.id] as string[] : undefined
    );

    setTimeout(() => {
      if (currentStep < questions.length - 1) {
        setCurrentStep((prev) => prev + 1);
      } else {
        // Calculate and show recommendation
        const result = calculateRecommendation(answers);
        setRecommendation(result);
        tracker?.trackQuizCompleted(result.id, result.name, result.matchScore);
      }
      setIsAnimating(false);
    }, 300);
  };

  const goBack = () => {
    setIsAnimating(true);
    setTimeout(() => {
      setCurrentStep((prev) => Math.max(0, prev - 1));
      setIsAnimating(false);
    }, 300);
  };

  const handleShare = (platform: string) => {
    if (recommendation) {
      tracker?.trackResultShared(platform, recommendation.id);
      // In real app, would open share dialog
      alert(`Sharing to ${platform}!`);
    }
  };

  const restart = () => {
    setCurrentStep(0);
    setAnswers({ 1: 10, 2: '', 3: [], 4: 100 });
    setRecommendation(null);
    tracker?.trackQuizStart(questions.length);
  };

  const currentQuestion = questions[currentStep];
  const canProceed = currentQuestion
    ? currentQuestion.type === 'slider' ||
      (currentQuestion.type === 'single' && answers[currentQuestion.id]) ||
      (currentQuestion.type === 'multi' && (answers[currentQuestion.id] as string[])?.length > 0)
    : false;

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-indigo-50 py-12 px-4">
      <div className="max-w-2xl mx-auto">
        {/* Header */}
        <div className="text-center mb-8">
          <div className="inline-flex items-center gap-2 px-4 py-2 bg-indigo-100 rounded-full text-indigo-700 text-sm font-medium mb-4">
            <span className="relative flex h-2 w-2">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-indigo-400 opacity-75"></span>
              <span className="relative inline-flex rounded-full h-2 w-2 bg-indigo-600"></span>
            </span>
            Smart Recommendation Engine
          </div>
          <h1 className="text-3xl font-bold text-gray-900 mb-2">
            Find Your Perfect Data Tool
          </h1>
          <p className="text-gray-600">
            Answer a few questions and we'll recommend the best solution for your needs
          </p>
        </div>

        {/* Main Card */}
        <div className="bg-white rounded-2xl shadow-xl p-8">
          {!recommendation ? (
            <>
              <ProgressIndicator current={currentStep} total={questions.length} />

              <div className={`transition-all duration-300 ${isAnimating ? 'opacity-0 transform translate-x-4' : 'opacity-100 transform translate-x-0'}`}>
                <h2 className="text-xl font-semibold text-gray-900 mb-2">
                  {currentQuestion.title}
                </h2>
                <p className="text-gray-500 mb-6">{currentQuestion.subtitle}</p>

                {currentQuestion.type === 'slider' && (
                  <SliderQuestion
                    question={currentQuestion}
                    value={answers[currentQuestion.id] as number}
                    onChange={(v) => handleSliderChange(currentQuestion.id, v)}
                  />
                )}

                {currentQuestion.type === 'single' && (
                  <SingleSelect
                    options={currentQuestion.options!}
                    selected={answers[currentQuestion.id] as string}
                    onChange={(id) => handleSingleSelect(currentQuestion.id, id)}
                  />
                )}

                {currentQuestion.type === 'multi' && (
                  <MultiSelect
                    options={currentQuestion.options!}
                    selected={answers[currentQuestion.id] as string[] || []}
                    onChange={(ids) => handleMultiSelect(currentQuestion.id, ids)}
                  />
                )}
              </div>

              {/* Navigation */}
              <div className="flex justify-between mt-8 pt-6 border-t">
                <button
                  onClick={goBack}
                  disabled={currentStep === 0}
                  className="px-5 py-2.5 text-gray-600 font-medium rounded-xl hover:bg-gray-100 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                >
                  ← Back
                </button>
                <button
                  onClick={goNext}
                  disabled={!canProceed}
                  className="px-5 py-2.5 bg-indigo-600 text-white font-medium rounded-xl hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                >
                  {currentStep === questions.length - 1 ? 'See Results' : 'Continue →'}
                </button>
              </div>
            </>
          ) : (
            <ResultCard
              recommendation={recommendation}
              onShare={handleShare}
              onRestart={restart}
            />
          )}
        </div>

        {/* Footer */}
        <p className="text-center text-gray-400 text-sm mt-6">
          Powered by Feature Launchpad Analytics
        </p>
      </div>
    </div>
  );
}
