export type JobState = "pending" | "running" | "succeeded" | "failed" | "dead";

export interface Job {
  id: string;
  type: string;
  state: JobState;
  attempts: number;
  max_attempts: number;
  last_error?: string;
  updated_at: string;
}

export interface Snapshot {
  queue: {
    counts: Partial<Record<JobState, number>>;
    recent: Job[];
  };
  workers: {
    workers: number;
    processed: number;
    failed: number;
  };
}
