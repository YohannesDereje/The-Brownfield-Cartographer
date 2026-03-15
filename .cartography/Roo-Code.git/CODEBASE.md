# CODEBASE
Architecture Overview:
This CODEBASE summary combines module structure and lineage metadata to highlight where the system's architecture is concentrated, how data flows from ingestion to sink points, and where operational risk is likely to accumulate due to dependency complexity, documentation drift, or high change velocity.
- Critical Path:
- C:\Users\Yohannes\Desktop\10 Academy\Week-4\cloned_repo_4\Roo-code-master-thinker\ellipsis.yaml (PageRank: 0.000000)
- C:\Users\Yohannes\Desktop\10 Academy\Week-4\cloned_repo_4\Roo-code-master-thinker\pnpm-lock.yaml (PageRank: 0.000000)
- C:\Users\Yohannes\Desktop\10 Academy\Week-4\cloned_repo_4\Roo-code-master-thinker\pnpm-workspace.yaml (PageRank: 0.000000)
- C:\Users\Yohannes\Desktop\10 Academy\Week-4\cloned_repo_4\Roo-code-master-thinker\.github\dependabot.yml (PageRank: 0.000000)
- C:\Users\Yohannes\Desktop\10 Academy\Week-4\cloned_repo_4\Roo-code-master-thinker\.github\actions\setup-node-pnpm\action.yml (PageRank: 0.000000)
- Data Sources & Sinks:
Ingestion points:
- source:public.runs
- source:public.taskMetrics
- source:public.tasks
- source:runs
- source:tasks
- source:tasks_language_exercise_idx
- source:tasks_run_id_runs_id_fk
- source:tasks_task_metrics_id_taskMetrics_id_fk
- source:toolErrors
- source:toolErrors_run_id_runs_id_fk
- source:toolErrors_task_id_tasks_id_fk
Output points:
- None detected
- Technical Debt:
Circular dependencies:
- None detected
Documentation Drift flags:
- None detected
- High-Velocity Core:
- C:\Users\Yohannes\Desktop\10 Academy\Week-4\cloned_repo_4\Roo-code-master-thinker\pnpm-lock.yaml (change_frequency: 44)
- C:\Users\Yohannes\Desktop\10 Academy\Week-4\cloned_repo_4\Roo-code-master-thinker\.github\workflows\cli-release.yml (change_frequency: 6)
- C:\Users\Yohannes\Desktop\10 Academy\Week-4\cloned_repo_4\Roo-code-master-thinker\.github\ISSUE_TEMPLATE\config.yml (change_frequency: 3)
- C:\Users\Yohannes\Desktop\10 Academy\Week-4\cloned_repo_4\Roo-code-master-thinker\pnpm-workspace.yaml (change_frequency: 2)
- C:\Users\Yohannes\Desktop\10 Academy\Week-4\cloned_repo_4\Roo-code-master-thinker\.github\workflows\website-preview.yml (change_frequency: 2)
- C:\Users\Yohannes\Desktop\10 Academy\Week-4\cloned_repo_4\Roo-code-master-thinker\.github\ISSUE_TEMPLATE\bug_report.yml (change_frequency: 1)
- C:\Users\Yohannes\Desktop\10 Academy\Week-4\cloned_repo_4\Roo-code-master-thinker\.github\workflows\website-deploy.yml (change_frequency: 1)
- C:\Users\Yohannes\Desktop\10 Academy\Week-4\cloned_repo_4\Roo-code-master-thinker\.orchestration\active_intents.yaml (change_frequency: 1)
- C:\Users\Yohannes\Desktop\10 Academy\Week-4\cloned_repo_4\Roo-code-master-thinker\packages\evals\src\db\migrations\0006_worried_spectrum.sql (change_frequency: 1)
- C:\Users\Yohannes\Desktop\10 Academy\Week-4\cloned_repo_4\Roo-code-master-thinker\ellipsis.yaml (change_frequency: 0)

