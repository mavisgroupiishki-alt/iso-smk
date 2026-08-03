import React, { useEffect, useMemo, useState } from 'react';
import { motion } from 'framer-motion';
import {
  AlertCircle,
  ArrowRight,
  BarChart3,
  Briefcase,
  CalendarClock,
  CalendarDays,
  CheckCircle2,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  ChevronUp,
  Clock,
  Database,
  Download,
  Edit3,
  ExternalLink,
  FileUp,
  FolderKanban,
  FolderPlus,
  GripVertical,
  History,
  Layers3,
  Link2,
  List,
  ListChecks,
  MessageSquare,
  Plus,
  RefreshCw,
  Search,
  ShieldCheck,
  Sparkles,
  Trash2,
  UserPlus,
  UserRound,
  Users,
  X,
} from 'lucide-react';
import { Bar, BarChart, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = import.meta.env.VITE_SUPABASE_URL;
const SUPABASE_ANON_KEY = import.meta.env.VITE_SUPABASE_ANON_KEY || import.meta.env.VITE_SUPABASE_PUBLISHABLE_KEY;
const supabase = SUPABASE_URL && SUPABASE_ANON_KEY ? createClient(SUPABASE_URL, SUPABASE_ANON_KEY) : null;

const TASKS_STORAGE_KEY = 'mavis_task_tracker_local_backup_v3';
const EMPLOYEES_STORAGE_KEY = 'mavis_task_tracker_employees_v1';
const PROJECTS_STORAGE_KEY = 'mavis_task_tracker_projects_v1';
const STAGES_STORAGE_KEY = 'mavis_task_tracker_project_stages_v1';
const RESCHEDULES_STORAGE_KEY = 'mavis_task_tracker_reschedules_v1';
const SECTIONS_STORAGE_KEY = 'mavis_task_tracker_sections_v1';
const CALENDAR_BACKUPS_STORAGE_KEY = 'mavis_task_tracker_calendar_backups_v1';

const DEFAULT_EMPLOYEES = [
  { id: 'default-sasha', name: 'Саша', role: 'Руководитель отдела продаж', color: '#7c3aed' },
  { id: 'default-tanya', name: 'Таня', role: 'Руководитель экспертного отдела', color: '#0284c7' },
  { id: 'default-anya', name: 'Аня', role: 'Проекты и автоматизация', color: '#ea580c' },
  { id: 'default-victoria', name: 'Виктория', role: 'Директор', color: '#059669' },
];

const EMPLOYEE_COLORS = ['#7c3aed', '#0284c7', '#ea580c', '#059669', '#db2777', '#4f46e5', '#0891b2', '#ca8a04'];
const PROJECT_COLORS = ['#7c3aed', '#2563eb', '#059669', '#ea580c', '#db2777', '#0891b2', '#4f46e5', '#be123c'];
const SECTION_COLORS = ['#0f766e', '#0369a1', '#7c3aed', '#c2410c', '#be123c', '#4f46e5', '#047857', '#a16207'];
const TASK_STATUSES = ['Ожидает', 'Новая', 'В работе', 'На проверке', 'Блокер', 'Готово'];
const PROJECT_STATUSES = ['Ожидает', 'В работе', 'На паузе', 'Готово'];
const PRIORITIES = ['Низкий', 'Средний', 'Высокий'];
const COMPLETED_TASK_STATUSES = new Set(['готово', 'сделано', 'выполнено', 'завершено', 'закрыто']);

function isTaskCompleted(taskOrStatus) {
  const status = typeof taskOrStatus === 'string' ? taskOrStatus : taskOrStatus?.status;
  return COMPLETED_TASK_STATUSES.has(String(status || '').trim().toLowerCase());
}

function todayIso() {
  const now = new Date();
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
}

function dateToIso(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return todayIso();
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
}

function addDays(dateIso, days) {
  const date = new Date(`${dateIso}T12:00:00`);
  date.setDate(date.getDate() + days);
  return dateToIso(date);
}

function getWeekStart(dateIso) {
  const date = new Date(`${dateIso}T12:00:00`);
  const day = date.getDay();
  date.setDate(date.getDate() + (day === 0 ? -6 : 1 - day));
  return dateToIso(date);
}

function getMonthBounds(dateIso) {
  const date = new Date(`${dateIso}T12:00:00`);
  return {
    first: dateToIso(new Date(date.getFullYear(), date.getMonth(), 1, 12)),
    last: dateToIso(new Date(date.getFullYear(), date.getMonth() + 1, 0, 12)),
  };
}

function getDateFilterBounds(mode, anchorDate) {
  if (mode === 'day') return { first: anchorDate, last: anchorDate };
  if (mode === 'week') {
    const first = getWeekStart(anchorDate);
    return { first, last: addDays(first, 6) };
  }
  if (mode === 'month') return getMonthBounds(anchorDate);
  return { first: '', last: '' };
}

function matchesDateFilter(value, mode, anchorDate) {
  if (mode === 'all') return true;
  const { first, last } = getDateFilterBounds(mode, anchorDate);
  return isWithinRange(value, first, last);
}

function dateFilterCaption(mode, anchorDate) {
  if (mode === 'all') return 'Все даты';
  const { first, last } = getDateFilterBounds(mode, anchorDate);
  if (mode === 'day') return formatDate(first);
  return `${formatDate(first)} — ${formatDate(last)}`;
}

function isWithinRange(value, start, end) {
  return Boolean(value && value >= start && value <= end);
}

function formatDate(dateIso) {
  if (!dateIso) return 'Без срока';
  return new Intl.DateTimeFormat('ru-RU', { day: '2-digit', month: 'short', year: 'numeric' }).format(new Date(`${dateIso}T12:00:00`));
}

function formatDateTime(value) {
  if (!value) return '—';
  return new Intl.DateTimeFormat('ru-RU', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  }).format(new Date(value));
}

function normalizeTime(value) {
  if (!value) return '';
  const match = String(value).match(/(\d{1,2}):(\d{2})/);
  if (!match) return '';
  return `${String(Math.min(23, Number(match[1]))).padStart(2, '0')}:${String(Math.min(59, Number(match[2]))).padStart(2, '0')}`;
}

function toMinutes(value) {
  const time = normalizeTime(value);
  if (!time) return null;
  const [hours, minutes] = time.split(':').map(Number);
  return hours * 60 + minutes;
}

function minutesToTime(value) {
  const minutes = Math.max(0, Math.min(23 * 60 + 59, Math.round(Number(value) || 0)));
  return `${String(Math.floor(minutes / 60)).padStart(2, '0')}:${String(minutes % 60).padStart(2, '0')}`;
}

function taskDurationMinutes(task) {
  const start = toMinutes(task?.start_time);
  const end = toMinutes(task?.end_time);
  if (start != null && end != null && end > start) return end - start;
  return 60;
}

function createSafetySnapshot(tasks, reason = 'calendar-change') {
  try {
    const previous = parseLocal(CALENDAR_BACKUPS_STORAGE_KEY, []);
    const snapshot = {
      created_at: new Date().toISOString(),
      reason,
      tasks,
    };
    localStorage.setItem(CALENDAR_BACKUPS_STORAGE_KEY, JSON.stringify([snapshot, ...previous].slice(0, 5)));
  } catch {
    // Резервная копия не должна блокировать основное действие.
  }
}

function hoursBetween(start, end) {
  const startMinutes = toMinutes(start);
  const endMinutes = toMinutes(end);
  if (startMinutes == null || endMinutes == null || endMinutes <= startMinutes) return null;
  return Math.round(((endMinutes - startMinutes) / 60) * 10) / 10;
}

function formatTime(value) {
  return normalizeTime(value) || 'без времени';
}

function normalizeOwner(value) {
  const name = String(value || '').trim();
  if (name === 'Алиса') return 'Саша';
  return name || 'Саша';
}

function normalizeResourceUrl(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const candidate = /^https?:\/\//i.test(raw) ? raw : `https://${raw}`;
  try {
    const parsed = new URL(candidate);
    return ['http:', 'https:'].includes(parsed.protocol) ? parsed.toString() : null;
  } catch {
    return null;
  }
}

function resourceLabel(value) {
  try {
    const host = new URL(value).hostname.replace(/^www\./, '');
    if (host.includes('docs.google.com')) return 'Открыть Google-документ';
    if (host.includes('drive.google.com')) return 'Открыть Google Drive';
    if (host.includes('miro.com')) return 'Открыть Miro';
    return `Открыть ${host}`;
  } catch {
    return 'Открыть материал';
  }
}

function normalizeEmployee(employee, index = 0) {
  return {
    id: employee.id || `local-employee-${Date.now()}-${index}`,
    name: normalizeOwner(employee.name),
    role: employee.role || 'Сотрудник',
    color: employee.color || EMPLOYEE_COLORS[index % EMPLOYEE_COLORS.length],
    created_at: employee.created_at || new Date().toISOString(),
  };
}

function normalizeProject(project, index = 0) {
  return {
    id: project.id || `local-project-${Date.now()}-${index}`,
    name: String(project.name || project.title || 'Без названия').trim(),
    description: project.description || '',
    owner: normalizeOwner(project.owner || 'Саша'),
    customer: String(project.customer || '').trim(),
    section_id: project.section_id || null,
    deadline: project.deadline || '',
    status: PROJECT_STATUSES.includes(project.status) ? project.status : 'В работе',
    color: project.color || PROJECT_COLORS[index % PROJECT_COLORS.length],
    created_at: project.created_at || new Date().toISOString(),
  };
}

function normalizeStage(stage, index = 0) {
  return {
    id: stage.id || `local-stage-${Date.now()}-${index}`,
    project_id: stage.project_id || null,
    title: String(stage.title || stage.name || 'Новый этап').trim(),
    description: stage.description || '',
    owner: normalizeOwner(stage.owner || 'Саша'),
    deadline: stage.deadline || '',
    sort_order: Number(stage.sort_order ?? index + 1),
    created_at: stage.created_at || new Date().toISOString(),
  };
}

function normalizeSection(section, index = 0) {
  return {
    id: section.id || `local-section-${Date.now()}-${index}`,
    name: String(section.name || section.title || 'Новый раздел').trim(),
    description: section.description || '',
    owner: normalizeOwner(section.owner || 'Саша'),
    color: section.color || SECTION_COLORS[index % SECTION_COLORS.length],
    created_at: section.created_at || new Date().toISOString(),
  };
}

function normalizeTask(task) {
  const start = normalizeTime(task.start_time || task.startTime || '');
  const end = normalizeTime(task.end_time || task.endTime || '');
  const calculatedHours = hoursBetween(start, end);
  return {
    id: task.id || `local-task-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    title: String(task.title || 'Без названия').trim(),
    owner: normalizeOwner(task.owner),
    deadline: task.deadline || todayIso(),
    period: ['day', 'week', 'month'].includes(task.period) ? task.period : 'day',
    status: TASK_STATUSES.includes(task.status) ? task.status : 'Новая',
    priority: PRIORITIES.includes(task.priority) ? task.priority : 'Средний',
    hours: Number(task.hours ?? calculatedHours ?? 1),
    start_time: start,
    end_time: end,
    block: task.block || '',
    result: task.result || '',
    comment: task.comment || '',
    resource_url: normalizeResourceUrl(task.resource_url || task.resourceUrl || '') || '',
    project_id: task.project_id || null,
    stage_id: task.stage_id || null,
    section_id: task.section_id || null,
    created_at: task.created_at || new Date().toISOString(),
  };
}

function normalizeReschedule(item) {
  return {
    id: item.id || `local-reschedule-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    task_id: item.task_id || null,
    task_title: item.task_title || 'Задача',
    project_id: item.project_id || null,
    stage_id: item.stage_id || null,
    old_deadline: item.old_deadline || '',
    new_deadline: item.new_deadline || '',
    changed_at: item.changed_at || new Date().toISOString(),
    changed_by: normalizeOwner(item.changed_by),
    reason: item.reason || '',
  };
}

function parseLocal(key, fallback) {
  try {
    const value = localStorage.getItem(key);
    return value ? JSON.parse(value) : fallback;
  } catch {
    return fallback;
  }
}

function isRemoteId(id) {
  const value = String(id || '');
  return value && !value.startsWith('local-') && !value.startsWith('default-') && !value.startsWith('sample-') && !value.startsWith('demo-') && !value.startsWith('import-');
}

function employeeInitials(name) {
  return String(name || '')
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part[0]?.toUpperCase())
    .join('') || '—';
}

function statusStyle(status) {
  if (status === 'Готово') return 'bg-emerald-100 text-emerald-700';
  if (status === 'В работе') return 'bg-blue-100 text-blue-700';
  if (status === 'На проверке') return 'bg-amber-100 text-amber-800';
  if (status === 'Блокер') return 'bg-rose-100 text-rose-700';
  if (status === 'На паузе') return 'bg-orange-100 text-orange-700';
  return 'bg-slate-100 text-slate-700';
}

function priorityStyle(priority) {
  if (priority === 'Высокий') return 'bg-rose-100 text-rose-700';
  if (priority === 'Средний') return 'bg-orange-100 text-orange-700';
  return 'bg-slate-100 text-slate-600';
}

function calculateProgress(items) {
  if (!items.length) return 0;
  return Math.round((items.filter((item) => item.status === 'Готово').length / items.length) * 100);
}

function deriveStatus(items, fallback = 'Ожидает') {
  if (!items.length) return fallback;
  if (items.every((item) => item.status === 'Готово')) return 'Готово';
  if (items.some((item) => item.status === 'Блокер')) return 'Блокер';
  if (items.some((item) => ['В работе', 'На проверке', 'Готово'].includes(item.status))) return 'В работе';
  return 'Ожидает';
}

function emptyTaskForm(selectedDate, owner, projectId = '', stageId = '') {
  return {
    title: '',
    owner: owner || 'Саша',
    deadline: selectedDate || todayIso(),
    period: 'day',
    status: 'Ожидает',
    priority: 'Средний',
    hours: 1,
    start_time: '09:00',
    end_time: '10:00',
    block: '',
    result: '',
    comment: '',
    resource_url: '',
    project_id: projectId || '',
    stage_id: stageId || '',
    section_id: '',
  };
}

const SAMPLE_PROJECTS = [
  normalizeProject({
    id: 'sample-project-beltech',
    name: 'Связка Битрикс + БелТехЭкспертиза',
    description: 'Настройка процесса передачи лидов, инструкции и ежедневной отчётности.',
    owner: 'Аня',
    deadline: addDays(todayIso(), 14),
    status: 'В работе',
    color: '#7c3aed',
  }),
];

const SAMPLE_STAGES = [
  normalizeStage({ id: 'sample-stage-1', project_id: 'sample-project-beltech', title: 'Подготовка CRM к работе', owner: 'Аня', deadline: addDays(todayIso(), 2), sort_order: 1 }),
  normalizeStage({ id: 'sample-stage-2', project_id: 'sample-project-beltech', title: 'Инструкция', owner: 'Аня', deadline: addDays(todayIso(), 5), sort_order: 2 }),
  normalizeStage({ id: 'sample-stage-3', project_id: 'sample-project-beltech', title: 'Финальная настройка', owner: 'Аня', deadline: addDays(todayIso(), 10), sort_order: 3 }),
];

const SAMPLE_TASKS = [
  normalizeTask({ id: 'sample-task-1', project_id: 'sample-project-beltech', stage_id: 'sample-stage-1', title: 'Созвониться с Иваном', owner: 'Аня', deadline: todayIso(), status: 'Готово', priority: 'Высокий', comment: 'Зафиксировать текущий процесс.' }),
  normalizeTask({ id: 'sample-task-2', project_id: 'sample-project-beltech', stage_id: 'sample-stage-1', title: 'Просмотреть Битрикс24 по доступу от Ивана', owner: 'Аня', deadline: todayIso(), status: 'Готово', comment: 'Нужен скрин воронки.' }),
  normalizeTask({ id: 'sample-task-3', project_id: 'sample-project-beltech', stage_id: 'sample-stage-2', title: 'Прописать инструкцию по созданию бизнес-процесса', owner: 'Аня', deadline: addDays(todayIso(), 2), status: 'В работе', comment: 'Добавить скрины и короткие пояснения.' }),
  normalizeTask({ id: 'sample-task-4', project_id: 'sample-project-beltech', stage_id: 'sample-stage-2', title: 'Согласовать время настройки с Иваном', owner: 'Аня', deadline: addDays(todayIso(), 3), status: 'Ожидает' }),
  normalizeTask({ id: 'sample-task-5', project_id: 'sample-project-beltech', stage_id: 'sample-stage-3', title: 'Тест на двух письмах', owner: 'Аня', deadline: addDays(todayIso(), 8), status: 'Ожидает' }),
  normalizeTask({ id: 'sample-task-6', project_id: 'sample-project-beltech', stage_id: 'sample-stage-3', title: 'Запуск в работу', owner: 'Аня', deadline: addDays(todayIso(), 10), status: 'Ожидает' }),
];

function Card({ children, className = '' }) {
  return <div className={`rounded-3xl border border-white/70 bg-white/90 shadow-[0_18px_55px_rgba(15,23,42,0.08)] backdrop-blur ${className}`}>{children}</div>;
}

function Metric({ icon: Icon, label, value, detail, tone = 'violet' }) {
  const tones = {
    violet: 'from-violet-500/15 to-fuchsia-500/5 text-violet-700 ring-violet-100',
    blue: 'from-sky-500/15 to-cyan-500/5 text-sky-700 ring-sky-100',
    amber: 'from-amber-500/20 to-orange-500/5 text-amber-700 ring-amber-100',
    emerald: 'from-emerald-500/15 to-teal-500/5 text-emerald-700 ring-emerald-100',
    rose: 'from-rose-500/15 to-red-500/5 text-rose-700 ring-rose-100',
    indigo: 'from-indigo-500/15 to-violet-500/5 text-indigo-700 ring-indigo-100',
  };
  return (
    <div className={`rounded-3xl bg-gradient-to-br p-4 ring-1 ${tones[tone] || tones.violet}`}>
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-sm font-medium text-slate-600">{label}</p>
          <p className="mt-1 text-3xl font-bold tracking-tight text-slate-950">{value}</p>
          {detail && <p className="mt-1 text-xs text-slate-500">{detail}</p>}
        </div>
        <span className="rounded-2xl bg-white/80 p-2.5 shadow-sm"><Icon className="h-5 w-5" /></span>
      </div>
    </div>
  );
}

function ProgressBar({ value, color = '#7c3aed' }) {
  return (
    <div className="flex items-center gap-3">
      <div className="h-2.5 flex-1 overflow-hidden rounded-full bg-slate-100">
        <div className="h-full rounded-full transition-all" style={{ width: `${Math.max(0, Math.min(100, value))}%`, backgroundColor: color }} />
      </div>
      <span className="w-10 text-right text-xs font-semibold text-slate-600">{value}%</span>
    </div>
  );
}

function TaskResourceLink({ url, compact = false }) {
  if (!url) return null;
  return (
    <a
      href={url}
      target="_blank"
      rel="noreferrer noopener"
      onClick={(event) => event.stopPropagation()}
      className={`inline-flex items-center gap-1.5 rounded-xl bg-sky-50 font-medium text-sky-700 hover:bg-sky-100 ${compact ? 'mt-1 px-2 py-1 text-xs' : 'px-3 py-2 text-sm'}`}
      title={url}
    >
      <Link2 className={compact ? 'h-3.5 w-3.5' : 'h-4 w-4'} />
      <span className="max-w-[260px] truncate">{resourceLabel(url)}</span>
      <ExternalLink className={compact ? 'h-3 w-3' : 'h-3.5 w-3.5'} />
    </a>
  );
}

function WeekCalendar({ tasks, unscheduledTasks, selectedDate, onEditTask, onMoveTask }) {
  const start = getWeekStart(selectedDate);
  const days = Array.from({ length: 7 }, (_, index) => addDays(start, index));
  const startHour = 8;
  const endHour = 20;
  const rowHeight = 38;
  const totalRows = (endHour - startHour) * 2;
  const gridHeight = totalRows * rowHeight;
  const timedTasks = tasks.filter((task) => task.start_time && task.end_time);
  const [draggedTaskId, setDraggedTaskId] = useState(null);
  const [dropPreview, setDropPreview] = useState(null);
  const [removeTimeActive, setRemoveTimeActive] = useState(false);

  function startDragging(event, task) {
    setDraggedTaskId(String(task.id));
    event.dataTransfer.effectAllowed = 'move';
    event.dataTransfer.setData('text/plain', String(task.id));
  }

  function finishDragging() {
    setDraggedTaskId(null);
    setDropPreview(null);
    setRemoveTimeActive(false);
  }

  function getDraggedTask(event) {
    const taskId = event.dataTransfer.getData('text/plain') || draggedTaskId;
    return [...tasks, ...unscheduledTasks].find((task) => String(task.id) === String(taskId));
  }

  function calculateDropTime(event) {
    const rect = event.currentTarget.getBoundingClientRect();
    const y = Math.max(0, Math.min(rect.height - 1, event.clientY - rect.top));
    const slotIndex = Math.max(0, Math.min(totalRows - 1, Math.floor(y / rowHeight)));
    return startHour * 60 + slotIndex * 30;
  }

  function handleDragOverDay(event, day) {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
    const task = getDraggedTask(event);
    if (!task) return;
    const startMinutes = calculateDropTime(event);
    const duration = taskDurationMinutes(task);
    const latestStart = endHour * 60 - 30;
    const safeStart = Math.min(startMinutes, latestStart);
    const safeEnd = Math.min(endHour * 60, safeStart + Math.max(30, duration));
    setDropPreview({
      day,
      startMinutes: safeStart,
      endMinutes: Math.max(safeStart + 30, safeEnd),
      color: task.color,
      title: task.title,
    });
  }

  async function handleDropOnDay(event, day) {
    event.preventDefault();
    const task = getDraggedTask(event);
    if (!task) return finishDragging();
    const startMinutes = dropPreview?.day === day ? dropPreview.startMinutes : calculateDropTime(event);
    const duration = taskDurationMinutes(task);
    const safeStart = Math.min(startMinutes, endHour * 60 - 30);
    const safeEnd = Math.min(endHour * 60, safeStart + Math.max(30, duration));
    await onMoveTask(task, {
      deadline: day,
      start_time: minutesToTime(safeStart),
      end_time: minutesToTime(Math.max(safeStart + 30, safeEnd)),
    });
    finishDragging();
  }

  async function handleRemoveTime(event) {
    event.preventDefault();
    const task = getDraggedTask(event);
    if (!task) return finishDragging();
    await onMoveTask(task, { deadline: task.deadline, start_time: '', end_time: '' });
    finishDragging();
  }

  return (
    <Card>
      <div className="p-5">
        <div className="mb-4 flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <h2 className="text-xl font-semibold">Календарь недели</h2>
            <p className="text-sm text-slate-500">Перетаскивайте задачи между днями и временем. Шаг планирования — 30 минут.</p>
          </div>
          <div className="flex flex-wrap items-center gap-2 text-sm text-slate-500">
            <span>{formatDate(days[0])} — {formatDate(days[6])}</span>
            <span className="rounded-full bg-violet-50 px-3 py-1 text-xs font-medium text-violet-700">Изменения сохраняются сразу</span>
          </div>
        </div>

        <div className="grid gap-4 xl:grid-cols-[280px_minmax(0,1fr)]">
          <aside className="space-y-3">
            <div className="rounded-2xl border border-violet-100 bg-gradient-to-br from-violet-50 to-sky-50 p-3">
              <div className="flex items-start gap-2">
                <span className="rounded-xl bg-white p-2 text-violet-600 shadow-sm"><GripVertical className="h-4 w-4" /></span>
                <div><p className="text-sm font-semibold text-slate-800">Задачи без времени</p><p className="mt-1 text-xs leading-5 text-slate-500">Возьмите задачу и перетащите её на нужный день и час.</p></div>
              </div>
            </div>

            <div className="max-h-[560px] space-y-2 overflow-y-auto pr-1">
              {unscheduledTasks.map((task) => (
                <button
                  key={task.id}
                  type="button"
                  draggable
                  onDragStart={(event) => startDragging(event, task)}
                  onDragEnd={finishDragging}
                  onClick={() => onEditTask(task)}
                  className={`group w-full rounded-2xl border bg-white p-3 text-left shadow-sm transition hover:-translate-y-0.5 hover:shadow-md ${String(draggedTaskId) === String(task.id) ? 'opacity-45' : ''}`}
                  style={{ borderLeft: `5px solid ${task.color}` }}
                >
                  <div className="flex items-start gap-2"><GripVertical className="mt-0.5 h-4 w-4 shrink-0 text-slate-300 group-hover:text-slate-500" /><div className="min-w-0"><p className="line-clamp-2 text-sm font-semibold text-slate-800">{task.title}</p><p className="mt-1 text-xs text-slate-500">{task.owner} · срок {formatDate(task.deadline)}</p></div></div>
                </button>
              ))}
              {unscheduledTasks.length === 0 && <div className="rounded-2xl border border-dashed p-5 text-center text-sm text-slate-500">Все задачи уже распределены по времени.</div>}
            </div>

            <div
              onDragOver={(event) => { event.preventDefault(); setRemoveTimeActive(true); }}
              onDragLeave={() => setRemoveTimeActive(false)}
              onDrop={handleRemoveTime}
              className={`rounded-2xl border-2 border-dashed p-4 text-center transition ${removeTimeActive ? 'border-rose-400 bg-rose-50 text-rose-700' : 'border-slate-200 bg-slate-50 text-slate-500'}`}
            >
              <Clock className="mx-auto h-5 w-5" /><p className="mt-1 text-sm font-medium">Снять время с задачи</p><p className="mt-1 text-xs">Перетащите сюда задачу из календаря.</p>
            </div>
          </aside>

          <div className="overflow-x-auto rounded-2xl border bg-white">
            <div className="min-w-[900px]">
              <div className="grid border-b bg-slate-50" style={{ gridTemplateColumns: '70px repeat(7, minmax(110px, 1fr))' }}>
                <div className="p-3 text-xs text-slate-500">Время</div>
                {days.map((day) => {
                  const date = new Date(`${day}T12:00:00`);
                  return (
                    <div key={day} className={`border-l p-2 text-center ${day === todayIso() ? 'bg-violet-50' : ''}`}>
                      <div className="text-xs uppercase text-slate-500">{new Intl.DateTimeFormat('ru-RU', { weekday: 'short' }).format(date)}</div>
                      <div className="text-lg font-bold">{date.getDate()}</div>
                    </div>
                  );
                })}
              </div>
              <div className="grid" style={{ gridTemplateColumns: '70px repeat(7, minmax(110px, 1fr))' }}>
                <div className="relative bg-slate-50" style={{ height: gridHeight }}>
                  {Array.from({ length: totalRows + 1 }, (_, index) => (
                    <div key={index} className="absolute left-0 right-0 border-t px-2 text-[11px] text-slate-500" style={{ top: index * rowHeight }}>
                      {index % 2 === 0 ? `${String(startHour + index / 2).padStart(2, '0')}:00` : ''}
                    </div>
                  ))}
                </div>
                {days.map((day) => (
                  <div
                    key={day}
                    onDragOver={(event) => handleDragOverDay(event, day)}
                    onDragLeave={(event) => { if (!event.currentTarget.contains(event.relatedTarget)) setDropPreview(null); }}
                    onDrop={(event) => handleDropOnDay(event, day)}
                    className={`relative border-l ${day === todayIso() ? 'bg-violet-50/30' : ''}`}
                    style={{ height: gridHeight }}
                  >
                    {Array.from({ length: totalRows + 1 }, (_, index) => <div key={index} className={`absolute left-0 right-0 border-t ${index % 2 ? 'border-slate-100' : 'border-slate-200'}`} style={{ top: index * rowHeight }} />)}
                    {dropPreview?.day === day && (
                      <div
                        className="pointer-events-none absolute left-1 right-1 z-20 rounded-lg border-2 border-dashed border-white/80 p-1.5 text-[11px] font-semibold text-white opacity-75 shadow-lg"
                        style={{
                          top: ((dropPreview.startMinutes - startHour * 60) / 30) * rowHeight,
                          height: Math.max(34, ((dropPreview.endMinutes - dropPreview.startMinutes) / 30) * rowHeight - 3),
                          backgroundColor: dropPreview.color,
                        }}
                      >
                        {minutesToTime(dropPreview.startMinutes)} · {dropPreview.title}
                      </div>
                    )}
                    {timedTasks.filter((task) => task.deadline === day).map((task) => {
                      const startMinutes = toMinutes(task.start_time) ?? startHour * 60;
                      const endMinutes = toMinutes(task.end_time) ?? startMinutes + 60;
                      const top = Math.max(0, ((startMinutes - startHour * 60) / 30) * rowHeight);
                      const height = Math.max(34, ((endMinutes - startMinutes) / 30) * rowHeight - 3);
                      return (
                        <button
                          key={task.id}
                          type="button"
                          draggable
                          onDragStart={(event) => startDragging(event, task)}
                          onDragEnd={finishDragging}
                          onClick={() => onEditTask(task)}
                          className={`absolute left-1 right-1 z-10 overflow-hidden rounded-lg p-1.5 text-left text-[11px] font-semibold leading-tight text-white shadow-sm transition hover:brightness-105 ${String(draggedTaskId) === String(task.id) ? 'opacity-40' : ''}`}
                          style={{ top, height, backgroundColor: task.color }}
                          title={`${task.title}\n${task.owner}\n${formatTime(task.start_time)}–${formatTime(task.end_time)}`}
                        >
                          <span className="flex items-start gap-1"><GripVertical className="mt-0.5 h-3 w-3 shrink-0 opacity-75" /><span>{formatTime(task.start_time)} · {task.title}</span></span>
                        </button>
                      );
                    })}
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      </div>
    </Card>
  );
}

export default function App() {
  const [tasks, setTasks] = useState([]);
  const [employees, setEmployees] = useState(DEFAULT_EMPLOYEES);
  const [projects, setProjects] = useState([]);
  const [stages, setStages] = useState([]);
  const [sections, setSections] = useState([]);
  const [reschedules, setReschedules] = useState([]);
  const [activeTab, setActiveTab] = useState('sections');
  const [selectedDate, setSelectedDate] = useState(todayIso());
  const [selectedEmployee, setSelectedEmployee] = useState('Все');
  const [search, setSearch] = useState('');
  const [taskFilter, setTaskFilter] = useState('all');
  const [statusFilter, setStatusFilter] = useState('all');
  const [sectionStatusFilters, setSectionStatusFilters] = useState({});
  const [dateFilterMode, setDateFilterMode] = useState('all');
  const [dateFilterDate, setDateFilterDate] = useState(todayIso());
  const [expandedSections, setExpandedSections] = useState({});
  const [expandedProjects, setExpandedProjects] = useState({});
  const [expandedStages, setExpandedStages] = useState({});
  const [transferFilter, setTransferFilter] = useState('current');
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState('');
  const [dataLoaded, setDataLoaded] = useState(false);

  const [isTaskModalOpen, setIsTaskModalOpen] = useState(false);
  const [isProjectModalOpen, setIsProjectModalOpen] = useState(false);
  const [isStageModalOpen, setIsStageModalOpen] = useState(false);
  const [isSectionModalOpen, setIsSectionModalOpen] = useState(false);
  const [isEmployeeModalOpen, setIsEmployeeModalOpen] = useState(false);
  const [editingTaskId, setEditingTaskId] = useState(null);
  const [editingProjectId, setEditingProjectId] = useState(null);
  const [editingStageId, setEditingStageId] = useState(null);
  const [editingSectionId, setEditingSectionId] = useState(null);

  const [taskForm, setTaskForm] = useState(emptyTaskForm(todayIso(), 'Саша'));
  const [projectForm, setProjectForm] = useState({ name: '', description: '', owner: 'Саша', customer: '', section_id: '', deadline: '', status: 'В работе', color: PROJECT_COLORS[0] });
  const [stageForm, setStageForm] = useState({ project_id: '', title: '', description: '', owner: 'Саша', deadline: '', sort_order: 1 });
  const [sectionForm, setSectionForm] = useState({ name: '', description: '', owner: 'Саша', color: SECTION_COLORS[0] });
  const [employeeForm, setEmployeeForm] = useState({ name: '', role: '', color: EMPLOYEE_COLORS[0] });
  const [deadlineChange, setDeadlineChange] = useState({ changed_by: 'Саша', reason: '' });

  const employeeNames = useMemo(() => employees.map((employee) => employee.name), [employees]);
  const currentWeekStart = getWeekStart(todayIso());
  const currentWeekEnd = addDays(currentWeekStart, 6);
  const nextWeekStart = addDays(currentWeekStart, 7);
  const nextWeekEnd = addDays(currentWeekStart, 13);

  async function loadData() {
    setDataLoaded(false);
    setLoading(true);
    setMessage('');

    const localEmployees = parseLocal(EMPLOYEES_STORAGE_KEY, DEFAULT_EMPLOYEES).map(normalizeEmployee);
    const localProjects = parseLocal(PROJECTS_STORAGE_KEY, supabase ? [] : SAMPLE_PROJECTS).map(normalizeProject);
    const localStages = parseLocal(STAGES_STORAGE_KEY, supabase ? [] : SAMPLE_STAGES).map(normalizeStage);
    const localSections = parseLocal(SECTIONS_STORAGE_KEY, []).map(normalizeSection);
    const localTasks = parseLocal(TASKS_STORAGE_KEY, supabase ? [] : SAMPLE_TASKS).map(normalizeTask);
    const localReschedules = parseLocal(RESCHEDULES_STORAGE_KEY, []).map(normalizeReschedule);

    if (!supabase) {
      setEmployees(localEmployees);
      setProjects(localProjects);
      setStages(localStages);
      setSections(localSections);
      setTasks(localTasks);
      setReschedules(localReschedules);
      setExpandedSections(Object.fromEntries(localSections.map((section) => [section.id, true])));
      setExpandedSections(Object.fromEntries(localSections.map((section) => [section.id, true])));
      setExpandedProjects(Object.fromEntries(localProjects.map((project) => [project.id, true])));
      setMessage('Локальный режим: данные сохраняются в этом браузере. Для общей работы команды подключите Supabase.');
      setDataLoaded(true);
      setLoading(false);
      return;
    }

    try {
      const [employeesResult, projectsResult, stagesResult, sectionsResult, tasksResult, reschedulesResult] = await Promise.all([
        supabase.from('employees').select('*').order('created_at', { ascending: true }),
        supabase.from('projects').select('*').order('created_at', { ascending: false }),
        supabase.from('project_stages').select('*').order('sort_order', { ascending: true }),
        supabase.from('task_sections').select('*').order('created_at', { ascending: true }),
        supabase.from('tasks').select('*').order('deadline', { ascending: true }).order('created_at', { ascending: false }),
        supabase.from('task_reschedules').select('*').order('changed_at', { ascending: false }),
      ]);

      if (tasksResult.error) throw tasksResult.error;
      const loadedEmployees = employeesResult.error || !employeesResult.data?.length ? localEmployees : employeesResult.data.map(normalizeEmployee);
      const loadedProjects = projectsResult.error ? localProjects : (projectsResult.data || []).map(normalizeProject);
      const loadedStages = stagesResult.error ? localStages : (stagesResult.data || []).map(normalizeStage);
      const loadedSections = sectionsResult.error ? localSections : (sectionsResult.data || []).map(normalizeSection);
      const loadedTasks = (tasksResult.data || []).map(normalizeTask);
      const loadedReschedules = reschedulesResult.error ? localReschedules : (reschedulesResult.data || []).map(normalizeReschedule);

      const employeeMap = new Map(loadedEmployees.map((employee) => [employee.name.toLowerCase(), employee]));
      loadedTasks.forEach((task, index) => {
        if (!employeeMap.has(task.owner.toLowerCase())) {
          const employee = normalizeEmployee({ name: task.owner, role: 'Сотрудник' }, loadedEmployees.length + index);
          loadedEmployees.push(employee);
          employeeMap.set(employee.name.toLowerCase(), employee);
        }
      });

      setEmployees(loadedEmployees);
      setProjects(loadedProjects);
      setStages(loadedStages);
      setSections(loadedSections);
      setTasks(loadedTasks);
      setReschedules(loadedReschedules);
      setExpandedSections(Object.fromEntries(loadedSections.map((section) => [section.id, true])));
      setExpandedProjects(Object.fromEntries(loadedProjects.map((project) => [project.id, true])));

      const migrationMissing = projectsResult.error || stagesResult.error || sectionsResult.error || reschedulesResult.error;
      setMessage(migrationMissing
        ? 'Основные данные загружены. Для разделов и заказчиков выполните файл supabase_hierarchy_sections_update.sql из архива.'
        : 'Данные проектов, задач и истории переносов загружены из общей базы Supabase.');
    } catch (error) {
      setEmployees(localEmployees);
      setProjects(localProjects);
      setStages(localStages);
      setSections(localSections);
      setTasks(localTasks);
      setReschedules(localReschedules);
      setExpandedProjects(Object.fromEntries(localProjects.map((project) => [project.id, true])));
      setMessage(`Общая база недоступна, открыта локальная копия. ${error.message}`);
    } finally {
      setDataLoaded(true);
      setLoading(false);
    }
  }

  useEffect(() => {
    loadData();
  }, []);

  useEffect(() => { if (dataLoaded) localStorage.setItem(TASKS_STORAGE_KEY, JSON.stringify(tasks)); }, [tasks, dataLoaded]);
  useEffect(() => { if (dataLoaded) localStorage.setItem(EMPLOYEES_STORAGE_KEY, JSON.stringify(employees)); }, [employees, dataLoaded]);
  useEffect(() => { if (dataLoaded) localStorage.setItem(PROJECTS_STORAGE_KEY, JSON.stringify(projects)); }, [projects, dataLoaded]);
  useEffect(() => { if (dataLoaded) localStorage.setItem(STAGES_STORAGE_KEY, JSON.stringify(stages)); }, [stages, dataLoaded]);
  useEffect(() => { if (dataLoaded) localStorage.setItem(SECTIONS_STORAGE_KEY, JSON.stringify(sections)); }, [sections, dataLoaded]);
  useEffect(() => { if (dataLoaded) localStorage.setItem(RESCHEDULES_STORAGE_KEY, JSON.stringify(reschedules)); }, [reschedules, dataLoaded]);

  useEffect(() => {
    if (selectedEmployee !== 'Все' && !employeeNames.includes(selectedEmployee)) setSelectedEmployee('Все');
  }, [selectedEmployee, employeeNames]);

  const taskById = useMemo(() => new Map(tasks.map((task) => [String(task.id), task])), [tasks]);
  const projectById = useMemo(() => new Map(projects.map((project) => [String(project.id), project])), [projects]);
  const stageById = useMemo(() => new Map(stages.map((stage) => [String(stage.id), stage])), [stages]);
  const sectionById = useMemo(() => new Map(sections.map((section) => [String(section.id), section])), [sections]);

  const filteredTasks = useMemo(() => {
    const query = search.trim().toLowerCase();
    return tasks.filter((task) => {
      const project = projectById.get(String(task.project_id));
      const stage = stageById.get(String(task.stage_id));
      const section = sectionById.get(String(project?.section_id || task.section_id));
      const matchesEmployee = selectedEmployee === 'Все' || task.owner === selectedEmployee;
      const matchesSearch = !query || `${task.title} ${task.owner} ${task.comment} ${task.resource_url} ${task.result} ${project?.name || ''} ${stage?.title || ''} ${section?.name || ''}`.toLowerCase().includes(query);
      const matchesFilter = taskFilter === 'all'
        || (taskFilter === 'active' && !isTaskCompleted(task))
        || (taskFilter === 'today' && task.deadline === todayIso())
        || (taskFilter === 'overdue' && task.deadline < todayIso() && !isTaskCompleted(task));
      const matchesStatus = statusFilter === 'all'
        || (statusFilter === 'active' && !isTaskCompleted(task))
        || (statusFilter === 'completed' && isTaskCompleted(task))
        || task.status === statusFilter;
      const matchesDate = matchesDateFilter(task.deadline, dateFilterMode, dateFilterDate);
      return matchesEmployee && matchesSearch && matchesFilter && matchesStatus && matchesDate;
    });
  }, [tasks, search, selectedEmployee, taskFilter, statusFilter, dateFilterMode, dateFilterDate, projectById, stageById, sectionById]);

  const filteredProjects = useMemo(() => {
    const query = search.trim().toLowerCase();
    return projects.filter((project) => {
      const allProjectTasks = tasks.filter((task) => String(task.project_id) === String(project.id));
      const visibleProjectTasks = filteredTasks.filter((task) => String(task.project_id) === String(project.id));
      const employeeMatch = selectedEmployee === 'Все' || project.owner === selectedEmployee || allProjectTasks.some((task) => task.owner === selectedEmployee);
      const projectSection = sectionById.get(String(project.section_id));
      const searchMatch = !query || `${project.name} ${project.description} ${project.owner} ${project.customer} ${projectSection?.name || ''}`.toLowerCase().includes(query)
        || allProjectTasks.some((task) => `${task.title} ${task.comment}`.toLowerCase().includes(query));
      const dateMatch = dateFilterMode === 'all' || matchesDateFilter(project.deadline, dateFilterMode, dateFilterDate) || visibleProjectTasks.length > 0;
      return employeeMatch && searchMatch && dateMatch;
    });
  }, [projects, tasks, filteredTasks, search, selectedEmployee, dateFilterMode, dateFilterDate, sectionById]);

  const filteredSections = useMemo(() => {
    const query = search.trim().toLowerCase();
    return sections.filter((section) => {
      const sectionProjects = projects.filter((project) => String(project.section_id) === String(section.id));
      const projectIds = new Set(sectionProjects.map((project) => String(project.id)));
      const allSectionTasks = tasks.filter((task) => projectIds.has(String(task.project_id)) || (!task.project_id && String(task.section_id) === String(section.id)));
      const visibleSectionTasks = filteredTasks.filter((task) => projectIds.has(String(task.project_id)) || (!task.project_id && String(task.section_id) === String(section.id)));
      const visibleSectionProjects = filteredProjects.filter((project) => String(project.section_id) === String(section.id));
      const employeeMatch = selectedEmployee === 'Все'
        || section.owner === selectedEmployee
        || sectionProjects.some((project) => project.owner === selectedEmployee)
        || allSectionTasks.some((task) => task.owner === selectedEmployee);
      const searchMatch = !query
        || `${section.name} ${section.description} ${section.owner}`.toLowerCase().includes(query)
        || sectionProjects.some((project) => `${project.name} ${project.description} ${project.customer}`.toLowerCase().includes(query))
        || allSectionTasks.some((task) => `${task.title} ${task.comment}`.toLowerCase().includes(query));
      const dateMatch = dateFilterMode === 'all' || visibleSectionTasks.length > 0 || visibleSectionProjects.length > 0;
      return employeeMatch && searchMatch && dateMatch;
    });
  }, [sections, projects, tasks, filteredTasks, filteredProjects, search, selectedEmployee, dateFilterMode]);

  const calendarTasks = useMemo(() => {
    const weekStart = getWeekStart(selectedDate);
    const weekEnd = addDays(weekStart, 6);
    return filteredTasks
      .filter((task) => !isTaskCompleted(task) && isWithinRange(task.deadline, weekStart, weekEnd))
      .map((task) => ({ ...task, color: employees.find((employee) => employee.name === task.owner)?.color || '#7c3aed' }));
  }, [filteredTasks, selectedDate, employees]);

  const unscheduledCalendarTasks = useMemo(() => filteredTasks
    .filter((task) => !isTaskCompleted(task) && (!task.start_time || !task.end_time))
    .sort((a, b) => String(a.deadline || '').localeCompare(String(b.deadline || '')))
    .map((task) => ({ ...task, color: employees.find((employee) => employee.name === task.owner)?.color || '#7c3aed' })), [filteredTasks, employees]);

  const currentWeekCarryovers = useMemo(() => reschedules.filter((item) =>
    isWithinRange(item.old_deadline, currentWeekStart, currentWeekEnd) && item.new_deadline > currentWeekEnd
  ), [reschedules, currentWeekStart, currentWeekEnd]);

  const nextWeekCarryovers = useMemo(() => currentWeekCarryovers.filter((item) =>
    isWithinRange(item.new_deadline, nextWeekStart, nextWeekEnd)
  ), [currentWeekCarryovers, nextWeekStart, nextWeekEnd]);

  const displayedTransfers = useMemo(() => {
    if (transferFilter === 'current') return currentWeekCarryovers;
    if (transferFilter === 'next') return nextWeekCarryovers;
    return reschedules;
  }, [transferFilter, currentWeekCarryovers, nextWeekCarryovers, reschedules]);

  const summary = useMemo(() => ({
    projects: projects.filter((project) => project.status !== 'Готово').length,
    tasks: tasks.length,
    active: tasks.filter((task) => !isTaskCompleted(task)).length,
    done: tasks.filter((task) => isTaskCompleted(task)).length,
    overdue: tasks.filter((task) => task.deadline < todayIso() && !isTaskCompleted(task)).length,
    carryovers: currentWeekCarryovers.length,
  }), [projects, tasks, currentWeekCarryovers]);

  const workload = useMemo(() => employees.map((employee) => {
    const personTasks = tasks.filter((task) => task.owner === employee.name && !isTaskCompleted(task));
    return {
      name: employee.name,
      tasks: personTasks.length,
      overdue: personTasks.filter((task) => task.deadline < todayIso()).length,
      color: employee.color,
    };
  }), [employees, tasks]);

  function resetAllFilters() {
    setSearch('');
    setSelectedEmployee('Все');
    setTaskFilter('all');
    setStatusFilter('all');
    setDateFilterMode('all');
    setDateFilterDate(todayIso());
    setSectionStatusFilters({});
    setMessage('Все фильтры сброшены.');
  }

  function matchesLocalTaskStatus(task, mode) {
    if (!mode || mode === 'all') return true;
    if (mode === 'active') return !isTaskCompleted(task);
    if (mode === 'completed') return isTaskCompleted(task);
    return task.status === mode;
  }

  async function moveProjectToSection(project, nextSectionId) {
    const previousProjects = projects;
    const sectionId = nextSectionId || null;
    setProjects((items) => items.map((item) => String(item.id) === String(project.id) ? { ...item, section_id: sectionId } : item));
    try {
      if (supabase && isRemoteId(project.id)) {
        const { error } = await supabase.from('projects').update({ section_id: isRemoteId(sectionId) ? sectionId : null }).eq('id', project.id);
        if (error) throw error;
      }
      const section = sectionById.get(String(sectionId));
      setMessage(section ? `Проект «${project.name}» перемещён в раздел «${section.name}».` : `Проект «${project.name}» вынесен из раздела.`);
    } catch (error) {
      setProjects(previousProjects);
      setMessage(`Не удалось переместить проект. Данные возвращены на место. ${error.message}`);
    }
  }

  function openTaskModal(task = null, projectId = '', stageId = '', sectionId = '') {
    if (task) {
      setEditingTaskId(task.id);
      setTaskForm({
        title: task.title,
        owner: task.owner,
        deadline: task.deadline,
        period: task.period,
        status: task.status,
        priority: task.priority,
        hours: task.hours,
        start_time: task.start_time || '',
        end_time: task.end_time || '',
        block: task.block || '',
        result: task.result || '',
        comment: task.comment || '',
        resource_url: task.resource_url || '',
        project_id: task.project_id || '',
        stage_id: task.stage_id || '',
        section_id: task.section_id || '',
      });
      setDeadlineChange({ changed_by: task.owner || employeeNames[0] || 'Саша', reason: '' });
    } else {
      const project = projectById.get(String(projectId));
      const stage = stageById.get(String(stageId));
      const section = sectionById.get(String(sectionId));
      setEditingTaskId(null);
      setTaskForm({
        ...emptyTaskForm(selectedDate, stage?.owner || project?.owner || section?.owner || (selectedEmployee !== 'Все' ? selectedEmployee : employeeNames[0]), projectId, stageId),
        section_id: sectionId || '',
      });
      setDeadlineChange({ changed_by: selectedEmployee !== 'Все' ? selectedEmployee : employeeNames[0] || 'Саша', reason: '' });
    }
    setIsTaskModalOpen(true);
  }

  function openProjectModal(project = null, sectionId = '') {
    if (project) {
      setEditingProjectId(project.id);
      setProjectForm({ name: project.name, description: project.description, owner: project.owner, customer: project.customer || '', section_id: project.section_id || '', deadline: project.deadline || '', status: project.status, color: project.color });
    } else {
      setEditingProjectId(null);
      setProjectForm({ name: '', description: '', owner: selectedEmployee !== 'Все' ? selectedEmployee : employeeNames[0] || 'Саша', customer: '', section_id: sectionId || '', deadline: '', status: 'В работе', color: PROJECT_COLORS[projects.length % PROJECT_COLORS.length] });
    }
    setIsProjectModalOpen(true);
  }

  function openStageModal(projectId, stage = null) {
    const project = projectById.get(String(projectId));
    if (stage) {
      setEditingStageId(stage.id);
      setStageForm({ project_id: stage.project_id, title: stage.title, description: stage.description, owner: stage.owner, deadline: stage.deadline || '', sort_order: stage.sort_order });
    } else {
      const projectStages = stages.filter((item) => String(item.project_id) === String(projectId));
      setEditingStageId(null);
      setStageForm({ project_id: projectId, title: '', description: '', owner: project?.owner || employeeNames[0] || 'Саша', deadline: project?.deadline || '', sort_order: projectStages.length + 1 });
    }
    setIsStageModalOpen(true);
  }

  async function saveProject() {
    if (!projectForm.name.trim()) {
      setMessage('Укажите название проекта.');
      return;
    }
    const payload = { ...projectForm, name: projectForm.name.trim(), description: projectForm.description.trim(), customer: projectForm.customer.trim(), section_id: projectForm.section_id || null, deadline: projectForm.deadline || null };

    try {
      if (editingProjectId) {
        const normalized = normalizeProject({ ...projectById.get(String(editingProjectId)), ...payload, id: editingProjectId });
        setProjects((previous) => previous.map((project) => String(project.id) === String(editingProjectId) ? normalized : project));
        if (supabase && isRemoteId(editingProjectId)) {
          const dbPayload = { ...payload, section_id: isRemoteId(payload.section_id) ? payload.section_id : null };
          const { error } = await supabase.from('projects').update(dbPayload).eq('id', editingProjectId);
          if (error) throw error;
        }
        setMessage('Проект обновлён.');
      } else if (supabase) {
        const dbPayload = { ...payload, section_id: isRemoteId(payload.section_id) ? payload.section_id : null };
        const { data, error } = await supabase.from('projects').insert(dbPayload).select().single();
        if (error) throw error;
        const project = normalizeProject(data, projects.length);
        setProjects((previous) => [project, ...previous]);
        setExpandedProjects((previous) => ({ ...previous, [project.id]: true }));
        setMessage('Проект создан в общей базе.');
      } else {
        const project = normalizeProject({ ...payload, id: `local-project-${Date.now()}` }, projects.length);
        setProjects((previous) => [project, ...previous]);
        setExpandedProjects((previous) => ({ ...previous, [project.id]: true }));
        setMessage('Проект создан локально.');
      }
      setIsProjectModalOpen(false);
    } catch (error) {
      if (!editingProjectId) {
        const project = normalizeProject({ ...payload, id: `local-project-${Date.now()}` }, projects.length);
        setProjects((previous) => [project, ...previous]);
        setExpandedProjects((previous) => ({ ...previous, [project.id]: true }));
      }
      setMessage(`Проект сохранён локально. Для иерархии разделов выполните supabase_hierarchy_sections_update.sql. ${error.message}`);
      setIsProjectModalOpen(false);
    }
  }

  async function saveStage() {
    if (!stageForm.project_id || !stageForm.title.trim()) {
      setMessage('Выберите проект и укажите название этапа.');
      return;
    }
    const payload = { ...stageForm, title: stageForm.title.trim(), description: stageForm.description.trim(), deadline: stageForm.deadline || null, sort_order: Number(stageForm.sort_order || 1) };

    try {
      if (editingStageId) {
        const normalized = normalizeStage({ ...stageById.get(String(editingStageId)), ...payload, id: editingStageId });
        setStages((previous) => previous.map((stage) => String(stage.id) === String(editingStageId) ? normalized : stage));
        if (supabase && isRemoteId(editingStageId)) {
          const { error } = await supabase.from('project_stages').update(payload).eq('id', editingStageId);
          if (error) throw error;
        }
        setMessage('Этап обновлён.');
      } else if (supabase && isRemoteId(stageForm.project_id)) {
        const { data, error } = await supabase.from('project_stages').insert(payload).select().single();
        if (error) throw error;
        const stage = normalizeStage(data, stages.length);
        setStages((previous) => [...previous, stage]);
        setExpandedStages((previous) => ({ ...previous, [stage.id]: true }));
        setMessage('Этап добавлен в проект.');
      } else {
        const stage = normalizeStage({ ...payload, id: `local-stage-${Date.now()}` }, stages.length);
        setStages((previous) => [...previous, stage]);
        setExpandedStages((previous) => ({ ...previous, [stage.id]: true }));
        setMessage('Этап добавлен локально.');
      }
      setIsStageModalOpen(false);
    } catch (error) {
      const stage = normalizeStage({ ...payload, id: `local-stage-${Date.now()}` }, stages.length);
      setStages((previous) => [...previous, stage]);
      setExpandedStages((previous) => ({ ...previous, [stage.id]: true }));
      setMessage(`Этап сохранён локально. Для общей базы выполните supabase_projects_update.sql. ${error.message}`);
      setIsStageModalOpen(false);
    }
  }

  function openSectionModal(section = null) {
    if (section) {
      setEditingSectionId(section.id);
      setSectionForm({ name: section.name, description: section.description, owner: section.owner, color: section.color });
    } else {
      setEditingSectionId(null);
      setSectionForm({
        name: '',
        description: '',
        owner: selectedEmployee !== 'Все' ? selectedEmployee : employeeNames[0] || 'Саша',
        color: SECTION_COLORS[sections.length % SECTION_COLORS.length],
      });
    }
    setIsSectionModalOpen(true);
  }

  async function saveSection() {
    if (!sectionForm.name.trim()) {
      setMessage('Укажите название раздела.');
      return;
    }
    const payload = {
      ...sectionForm,
      name: sectionForm.name.trim(),
      description: sectionForm.description.trim(),
    };

    try {
      if (editingSectionId) {
        const normalized = normalizeSection({ ...sectionById.get(String(editingSectionId)), ...payload, id: editingSectionId });
        setSections((previous) => previous.map((section) => String(section.id) === String(editingSectionId) ? normalized : section));
        if (supabase && isRemoteId(editingSectionId)) {
          const { error } = await supabase.from('task_sections').update(payload).eq('id', editingSectionId);
          if (error) throw error;
        }
        setMessage('Раздел обновлён.');
      } else if (supabase) {
        const { data, error } = await supabase.from('task_sections').insert(payload).select().single();
        if (error) throw error;
        setSections((previous) => [...previous, normalizeSection(data, previous.length)]);
        setMessage('Раздел создан в общей базе.');
      } else {
        setSections((previous) => [...previous, normalizeSection({ ...payload, id: `local-section-${Date.now()}` }, previous.length)]);
        setMessage('Раздел создан локально.');
      }
      setIsSectionModalOpen(false);
    } catch (error) {
      if (!editingSectionId) {
        setSections((previous) => [...previous, normalizeSection({ ...payload, id: `local-section-${Date.now()}` }, previous.length)]);
      }
      setMessage(`Раздел сохранён локально. Выполните supabase_hierarchy_sections_update.sql. ${error.message}`);
      setIsSectionModalOpen(false);
    }
  }

  async function deleteSection(section) {
    const linkedProjects = projects.filter((project) => String(project.section_id) === String(section.id));
    const linkedTasks = tasks.filter((task) => !task.project_id && String(task.section_id) === String(section.id));
    if (linkedProjects.length || linkedTasks.length) {
      setMessage(`Нельзя удалить раздел «${section.name}»: внутри ${linkedProjects.length} проектов и ${linkedTasks.length} задач без проекта. Сначала перенесите их.`);
      return;
    }
    const previous = sections;
    setSections((items) => items.filter((item) => String(item.id) !== String(section.id)));
    try {
      if (supabase && isRemoteId(section.id)) {
        const { error } = await supabase.from('task_sections').delete().eq('id', section.id);
        if (error) throw error;
      }
      setMessage('Раздел удалён.');
    } catch (error) {
      setSections(previous);
      setMessage(`Не удалось удалить раздел: ${error.message}`);
    }
  }

  async function createRescheduleRecord(previousTask, updatedTask, metadata = {}) {
    if (!previousTask || previousTask.deadline === updatedTask.deadline) return;
    const record = normalizeReschedule({
      task_id: updatedTask.id,
      task_title: updatedTask.title,
      project_id: updatedTask.project_id || null,
      stage_id: updatedTask.stage_id || null,
      old_deadline: previousTask.deadline,
      new_deadline: updatedTask.deadline,
      changed_at: new Date().toISOString(),
      changed_by: metadata.changed_by || deadlineChange.changed_by || updatedTask.owner,
      reason: metadata.reason ?? deadlineChange.reason.trim(),
    });

    setReschedules((previous) => [record, ...previous]);
    if (supabase && isRemoteId(updatedTask.id)) {
      const payload = {
        task_id: updatedTask.id,
        task_title: updatedTask.title,
        project_id: isRemoteId(updatedTask.project_id) ? updatedTask.project_id : null,
        stage_id: isRemoteId(updatedTask.stage_id) ? updatedTask.stage_id : null,
        old_deadline: previousTask.deadline,
        new_deadline: updatedTask.deadline,
        changed_at: record.changed_at,
        changed_by: record.changed_by,
        reason: record.reason,
      };
      const { data, error } = await supabase.from('task_reschedules').insert(payload).select().single();
      if (!error && data) {
        setReschedules((previous) => previous.map((item) => item.id === record.id ? normalizeReschedule(data) : item));
      }
    }
  }


  async function moveTaskInCalendar(task, schedule) {
    const currentTask = taskById.get(String(task.id));
    if (!currentTask) return;

    const nextStart = normalizeTime(schedule.start_time || '');
    const nextEnd = normalizeTime(schedule.end_time || '');
    const nextDeadline = schedule.deadline || currentTask.deadline;
    const updatedTask = normalizeTask({
      ...currentTask,
      deadline: nextDeadline,
      start_time: nextStart,
      end_time: nextEnd,
      hours: currentTask.hours,
    });

    if (
      updatedTask.deadline === currentTask.deadline
      && updatedTask.start_time === currentTask.start_time
      && updatedTask.end_time === currentTask.end_time
    ) return;

    const previousTasks = tasks;
    createSafetySnapshot(tasks, `До перемещения задачи «${currentTask.title}»`);
    setTasks((items) => items.map((item) => String(item.id) === String(currentTask.id) ? updatedTask : item));

    try {
      if (supabase && isRemoteId(currentTask.id)) {
        const { error } = await supabase.from('tasks').update({
          deadline: updatedTask.deadline,
          start_time: updatedTask.start_time || null,
          end_time: updatedTask.end_time || null,
        }).eq('id', currentTask.id);
        if (error) throw error;
      }

      if (currentTask.deadline !== updatedTask.deadline) {
        await createRescheduleRecord(currentTask, updatedTask, {
          changed_by: selectedEmployee !== 'Все' ? selectedEmployee : updatedTask.owner,
          reason: 'Перенос выполнен перетаскиванием в календаре',
        });
      }

      const timeText = updatedTask.start_time
        ? `${formatTime(updatedTask.start_time)}–${formatTime(updatedTask.end_time)}`
        : 'без времени';
      setMessage(`Задача «${updatedTask.title}» перенесена на ${formatDate(updatedTask.deadline)}, ${timeText}. Изменение сохранено.`);
    } catch (error) {
      setTasks(previousTasks);
      setMessage(`Не удалось сохранить перенос. Задача возвращена на прежнее место, данные не потеряны. ${error.message}`);
    }
  }

  function downloadBackup() {
    const payload = {
      exported_at: new Date().toISOString(),
      source: 'Mavis Task Tracker',
      employees,
      projects,
      stages,
      sections,
      tasks,
      reschedules,
    };
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `mavis-task-tracker-backup-${todayIso()}.json`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
    setMessage('Резервная копия проектов и задач скачана на компьютер.');
  }

  async function saveTask() {
    if (!taskForm.title.trim()) {
      setMessage('Укажите название задачи.');
      return;
    }
    if (taskForm.stage_id && !taskForm.project_id) {
      setMessage('Этап можно выбрать только внутри проекта.');
      return;
    }
    if (taskForm.project_id && taskForm.section_id) {
      setMessage('Задачу можно привязать либо к проекту, либо к разделу.');
      return;
    }
    const resourceUrl = normalizeResourceUrl(taskForm.resource_url);
    if (String(taskForm.resource_url || '').trim() && !resourceUrl) {
      setMessage('Проверьте ссылку на материалы. Поддерживаются ссылки, начинающиеся с http:// или https://.');
      return;
    }

    const calculatedHours = hoursBetween(taskForm.start_time, taskForm.end_time);
    const payload = {
      title: taskForm.title.trim(),
      owner: taskForm.owner,
      deadline: taskForm.deadline,
      period: taskForm.period,
      status: taskForm.status,
      priority: taskForm.priority,
      hours: Number(taskForm.hours || calculatedHours || 1),
      start_time: taskForm.start_time || null,
      end_time: taskForm.end_time || null,
      block: taskForm.block.trim(),
      result: taskForm.result.trim(),
      comment: taskForm.comment.trim(),
      resource_url: resourceUrl || '',
      project_id: taskForm.project_id || null,
      stage_id: taskForm.stage_id || null,
      section_id: taskForm.section_id || null,
    };

    try {
      if (editingTaskId) {
        const previousTask = taskById.get(String(editingTaskId));
        const updatedTask = normalizeTask({ ...previousTask, ...payload, id: editingTaskId });
        setTasks((previous) => previous.map((task) => String(task.id) === String(editingTaskId) ? updatedTask : task));
        if (supabase && isRemoteId(editingTaskId)) {
          const dbPayload = {
            ...payload,
            project_id: isRemoteId(payload.project_id) ? payload.project_id : null,
            stage_id: isRemoteId(payload.stage_id) ? payload.stage_id : null,
            section_id: isRemoteId(payload.section_id) ? payload.section_id : null,
          };
          const { error } = await supabase.from('tasks').update(dbPayload).eq('id', editingTaskId);
          if (error) throw error;
        }
        await createRescheduleRecord(previousTask, updatedTask);
        setMessage(previousTask?.deadline !== updatedTask.deadline ? 'Задача сохранена, перенос срока записан в историю.' : 'Задача обновлена.');
      } else if (supabase && (!payload.project_id || isRemoteId(payload.project_id)) && (!payload.stage_id || isRemoteId(payload.stage_id)) && (!payload.section_id || isRemoteId(payload.section_id))) {
        const { data, error } = await supabase.from('tasks').insert(payload).select().single();
        if (error) throw error;
        setTasks((previous) => [normalizeTask(data), ...previous]);
        setMessage('Задача создана в общей базе.');
      } else {
        const task = normalizeTask({ ...payload, id: `local-task-${Date.now()}` });
        setTasks((previous) => [task, ...previous]);
        setMessage('Задача создана локально.');
      }
      setIsTaskModalOpen(false);
      setEditingTaskId(null);
    } catch (error) {
      if (!editingTaskId) {
        const task = normalizeTask({ ...payload, id: `local-task-${Date.now()}` });
        setTasks((previous) => [task, ...previous]);
      }
      setMessage(`Изменение сохранено локально. Проверьте миграцию Supabase. ${error.message}`);
      setIsTaskModalOpen(false);
    }
  }

  async function updateTaskStatus(taskId, status) {
    const previous = tasks;
    setTasks((items) => items.map((task) => String(task.id) === String(taskId) ? { ...task, status } : task));
    try {
      if (supabase && isRemoteId(taskId)) {
        const { error } = await supabase.from('tasks').update({ status }).eq('id', taskId);
        if (error) throw error;
      }
    } catch (error) {
      setTasks(previous);
      setMessage(`Не удалось изменить статус: ${error.message}`);
    }
  }

  async function deleteTask(task) {
    const previous = tasks;
    setTasks((items) => items.filter((item) => String(item.id) !== String(task.id)));
    try {
      if (supabase && isRemoteId(task.id)) {
        const { error } = await supabase.from('tasks').delete().eq('id', task.id);
        if (error) throw error;
      }
      setMessage('Задача удалена.');
    } catch (error) {
      setTasks(previous);
      setMessage(`Не удалось удалить задачу: ${error.message}`);
    }
  }

  async function deleteStage(stage) {
    const stageTasks = tasks.filter((task) => String(task.stage_id) === String(stage.id));
    if (stageTasks.length) {
      setMessage(`Нельзя удалить этап «${stage.title}»: внутри ${stageTasks.length} задач. Сначала перенесите или удалите их.`);
      return;
    }
    const previous = stages;
    setStages((items) => items.filter((item) => String(item.id) !== String(stage.id)));
    try {
      if (supabase && isRemoteId(stage.id)) {
        const { error } = await supabase.from('project_stages').delete().eq('id', stage.id);
        if (error) throw error;
      }
      setMessage('Этап удалён.');
    } catch (error) {
      setStages(previous);
      setMessage(`Не удалось удалить этап: ${error.message}`);
    }
  }

  async function deleteProject(project) {
    const projectTasks = tasks.filter((task) => String(task.project_id) === String(project.id));
    const projectStages = stages.filter((stage) => String(stage.project_id) === String(project.id));
    if (projectTasks.length || projectStages.length) {
      setMessage(`Нельзя удалить проект «${project.name}»: в нём ${projectStages.length} этапов и ${projectTasks.length} задач.`);
      return;
    }
    const previous = projects;
    setProjects((items) => items.filter((item) => String(item.id) !== String(project.id)));
    try {
      if (supabase && isRemoteId(project.id)) {
        const { error } = await supabase.from('projects').delete().eq('id', project.id);
        if (error) throw error;
      }
      setMessage('Проект удалён.');
    } catch (error) {
      setProjects(previous);
      setMessage(`Не удалось удалить проект: ${error.message}`);
    }
  }

  async function deleteEmployee(employee) {
    const linkedTasks = tasks.filter((task) => task.owner === employee.name).length;
    const linkedProjects = projects.filter((project) => project.owner === employee.name).length;
    const linkedStages = stages.filter((stage) => stage.owner === employee.name).length;
    const linkedSections = sections.filter((section) => section.owner === employee.name).length;
    if (linkedTasks || linkedProjects || linkedStages || linkedSections) {
      setMessage(`Нельзя удалить ${employee.name}: сотрудник указан в ${linkedProjects} проектах, ${linkedStages} этапах, ${linkedSections} разделах и ${linkedTasks} задачах. Сначала переназначьте ответственность.`);
      return;
    }
    const previous = employees;
    setEmployees((items) => items.filter((item) => String(item.id) !== String(employee.id)));
    try {
      if (supabase && isRemoteId(employee.id)) {
        const { error } = await supabase.from('employees').delete().eq('id', employee.id);
        if (error) throw error;
      }
      setMessage(`Сотрудник ${employee.name} удалён.`);
    } catch (error) {
      setEmployees(previous);
      setMessage(`Не удалось удалить сотрудника: ${error.message}`);
    }
  }

  async function addEmployee() {
    const name = normalizeOwner(employeeForm.name);
    if (!name) {
      setMessage('Укажите имя сотрудника.');
      return;
    }
    if (employeeNames.some((item) => item.toLowerCase() === name.toLowerCase())) {
      setMessage('Сотрудник с таким именем уже существует.');
      return;
    }
    const payload = { name, role: employeeForm.role.trim() || 'Сотрудник', color: employeeForm.color };
    try {
      if (supabase) {
        const { data, error } = await supabase.from('employees').insert(payload).select().single();
        if (error) throw error;
        setEmployees((previous) => [...previous, normalizeEmployee(data, previous.length)]);
        setMessage('Сотрудник добавлен в общую базу.');
      } else {
        setEmployees((previous) => [...previous, normalizeEmployee({ ...payload, id: `local-employee-${Date.now()}` }, previous.length)]);
        setMessage('Сотрудник добавлен локально.');
      }
      setEmployeeForm({ name: '', role: '', color: EMPLOYEE_COLORS[(employees.length + 1) % EMPLOYEE_COLORS.length] });
      setIsEmployeeModalOpen(false);
    } catch (error) {
      setEmployees((previous) => [...previous, normalizeEmployee({ ...payload, id: `local-employee-${Date.now()}` }, previous.length)]);
      setMessage(`Сотрудник добавлен локально. ${error.message}`);
      setIsEmployeeModalOpen(false);
    }
  }

  async function importProjectTable(event) {
    const file = event.target.files?.[0];
    event.target.value = '';
    if (!file) return;
    try {
      const XLSX = await import('xlsx');
      const workbook = XLSX.read(await file.arrayBuffer(), { type: 'array', cellDates: true });
      const sheet = workbook.Sheets[workbook.SheetNames[0]];
      const rows = XLSX.utils.sheet_to_json(sheet, { defval: '' });
      if (!rows.length) throw new Error('В таблице нет строк.');

      const projectName = file.name.replace(/\.(xlsx|xls|csv)$/i, '') || 'Импортированный проект';
      const project = normalizeProject({ id: `local-project-${Date.now()}`, name: projectName, section_id: null, owner: selectedEmployee !== 'Все' ? selectedEmployee : employeeNames[0], deadline: '', status: 'В работе', color: PROJECT_COLORS[projects.length % PROJECT_COLORS.length] }, projects.length);
      const newStages = [];
      const newTasks = [];
      const stageMap = new Map();

      rows.forEach((row, index) => {
        const stageTitle = String(row['Этап'] || row['этап'] || row['Stage'] || 'Без этапа').trim() || 'Без этапа';
        const title = String(row['Задача'] || row['задача'] || row['Task'] || '').trim();
        if (!title) return;
        if (!stageMap.has(stageTitle)) {
          const stage = normalizeStage({ id: `local-stage-${Date.now()}-${stageMap.size}`, project_id: project.id, title: stageTitle, owner: normalizeOwner(row['Ответственный'] || employeeNames[0]), sort_order: stageMap.size + 1 }, stageMap.size);
          stageMap.set(stageTitle, stage);
          newStages.push(stage);
        }
        const rawDeadline = row['Дедлайн'] || row['Срок'] || row['deadline'];
        const deadline = rawDeadline instanceof Date ? dateToIso(rawDeadline) : String(rawDeadline || todayIso()).slice(0, 10);
        newTasks.push(normalizeTask({
          id: `local-task-${Date.now()}-${index}`,
          project_id: project.id,
          stage_id: stageMap.get(stageTitle).id,
          title,
          owner: normalizeOwner(row['Ответственный'] || employeeNames[0]),
          deadline: /^\d{4}-\d{2}-\d{2}$/.test(deadline) ? deadline : todayIso(),
          status: TASK_STATUSES.includes(row['Статус']) ? row['Статус'] : 'Ожидает',
          priority: PRIORITIES.includes(row['Приоритет']) ? row['Приоритет'] : 'Средний',
          comment: String(row['Комментарий'] || ''),
          resource_url: String(row['Ссылка'] || row['Материалы'] || row['Ссылка на материалы'] || row['URL'] || ''),
          result: String(row['Результат'] || ''),
        }));
      });

      setProjects((previous) => [project, ...previous]);
      setStages((previous) => [...previous, ...newStages]);
      setTasks((previous) => [...newTasks, ...previous]);
      setExpandedProjects((previous) => ({ ...previous, [project.id]: true }));
      setMessage(`Импортировано: 1 проект, ${newStages.length} этапов и ${newTasks.length} задач. Данные сохранены локально; при необходимости отредактируйте проект.`);
      setActiveTab('projects');
    } catch (error) {
      setMessage(`Не удалось импортировать таблицу: ${error.message}`);
    }
  }

  const selectedProjectStages = stages.filter((stage) => String(stage.project_id) === String(taskForm.project_id)).sort((a, b) => a.sort_order - b.sort_order);
  const editingTask = editingTaskId ? taskById.get(String(editingTaskId)) : null;
  const deadlineWasChanged = Boolean(editingTask && taskForm.deadline !== editingTask.deadline);

  function renderHierarchyProject(project, localStatusMode = 'active') {
    const projectStages = stages.filter((stage) => String(stage.project_id) === String(project.id)).sort((a, b) => a.sort_order - b.sort_order);
    const allProjectTasks = tasks.filter((task) => String(task.project_id) === String(project.id));
    const visibleProjectTasks = filteredTasks
      .filter((task) => String(task.project_id) === String(project.id))
      .filter((task) => matchesLocalTaskStatus(task, localStatusMode));
    const projectProgress = calculateProgress(allProjectTasks);
    const projectStatus = deriveStatus(allProjectTasks, project.status);
    const expanded = expandedProjects[project.id] !== false;
    const tasksWithoutStage = visibleProjectTasks.filter((task) => !task.stage_id);

    return (
      <div key={project.id} className="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
        <div className="h-1.5" style={{ backgroundColor: project.color }} />
        <div className="p-4">
          <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
            <button type="button" onClick={() => setExpandedProjects((previous) => ({ ...previous, [project.id]: !expanded }))} className="flex min-w-0 flex-1 items-start gap-3 text-left">
              <span className="rounded-xl p-2 text-white" style={{ backgroundColor: project.color }}><FolderKanban className="h-4 w-4" /></span>
              <span className="min-w-0 flex-1">
                <span className="flex flex-wrap items-center gap-2"><b className="text-base">{project.name}</b><span className={`rounded-full px-2.5 py-1 text-xs ${statusStyle(projectStatus)}`}>{projectStatus}</span></span>
                <span className="mt-1 block text-sm text-slate-500">{project.description || 'Описание проекта не заполнено'}</span>
                <span className="mt-2 block max-w-xl"><ProgressBar value={projectProgress} color={project.color} /></span>
              </span>
              {expanded ? <ChevronUp className="mt-1 h-4 w-4 text-slate-400" /> : <ChevronDown className="mt-1 h-4 w-4 text-slate-400" />}
            </button>
            <div className="flex flex-wrap items-center gap-2 text-xs">
              <span className="rounded-lg bg-slate-100 px-2.5 py-1.5"><b>{project.owner}</b> · {project.deadline ? formatDate(project.deadline) : 'без срока'}</span>
              {project.customer && <span className="rounded-lg bg-amber-50 px-2.5 py-1.5 text-amber-800">Заказчик: {project.customer}</span>}
              <span className="rounded-lg bg-slate-100 px-2.5 py-1.5">{allProjectTasks.filter((task) => !isTaskCompleted(task)).length} активных · {allProjectTasks.length} всего</span>
              <select value={project.section_id || ''} onChange={(event) => moveProjectToSection(project, event.target.value)} className="rounded-lg border border-cyan-200 bg-cyan-50 px-2.5 py-1.5 font-medium text-cyan-800"><option value="">Без раздела</option>{sections.map((section) => <option key={section.id} value={section.id}>{section.name}</option>)}</select>
              <button type="button" onClick={() => openStageModal(project.id)} className="inline-flex items-center rounded-lg bg-sky-50 px-2.5 py-1.5 font-medium text-sky-700"><Layers3 className="mr-1 h-3.5 w-3.5" />Этап</button>
              <button type="button" onClick={() => openTaskModal(null, project.id)} className="inline-flex items-center rounded-lg bg-violet-600 px-2.5 py-1.5 font-medium text-white"><Plus className="mr-1 h-3.5 w-3.5" />Задача</button>
              <button type="button" onClick={() => openProjectModal(project)} className="rounded-lg border p-1.5 text-slate-600"><Edit3 className="h-3.5 w-3.5" /></button>
            </div>
          </div>

          {expanded && (
            <div className="mt-4 space-y-3">
              {projectStages.map((stage) => {
                const allStageTasks = allProjectTasks.filter((task) => String(task.stage_id) === String(stage.id));
                const stageTasks = visibleProjectTasks.filter((task) => String(task.stage_id) === String(stage.id));
                const stageProgress = calculateProgress(allStageTasks);
                const stageStatus = deriveStatus(allStageTasks);
                const stageExpanded = expandedStages[stage.id] !== false;
                return (
                  <div key={stage.id} className="overflow-hidden rounded-xl border border-slate-200 bg-slate-50/80">
                    <div className="flex flex-col gap-2 p-3 lg:flex-row lg:items-center lg:justify-between">
                      <button type="button" onClick={() => setExpandedStages((previous) => ({ ...previous, [stage.id]: !stageExpanded }))} className="flex min-w-0 flex-1 items-center gap-2 text-left">
                        <span className="rounded-lg bg-white p-1.5 text-violet-600"><Layers3 className="h-3.5 w-3.5" /></span>
                        <span className="min-w-0 flex-1"><span className="flex flex-wrap items-center gap-2"><b className="text-sm">{stage.sort_order}. {stage.title}</b><span className={`rounded-full px-2 py-0.5 text-[11px] ${statusStyle(stageStatus)}`}>{stageStatus}</span></span><span className="mt-1 block max-w-md"><ProgressBar value={stageProgress} color={project.color} /></span></span>
                        {stageExpanded ? <ChevronUp className="h-4 w-4 text-slate-400" /> : <ChevronDown className="h-4 w-4 text-slate-400" />}
                      </button>
                      <div className="flex flex-wrap items-center gap-2 text-xs text-slate-600"><span className="rounded-lg bg-white px-2 py-1">{stage.owner}</span><span className="rounded-lg bg-white px-2 py-1">{stage.deadline ? formatDate(stage.deadline) : 'Без срока'}</span><span className="rounded-lg bg-white px-2 py-1">{stageTasks.length} показано / {allStageTasks.length}</span><button type="button" onClick={() => openTaskModal(null, project.id, stage.id)} className="rounded-lg bg-violet-600 px-2.5 py-1 font-medium text-white">+ Задача</button><button type="button" onClick={() => openStageModal(project.id, stage)} className="rounded-lg bg-white p-1.5"><Edit3 className="h-3.5 w-3.5" /></button></div>
                    </div>
                    {stageExpanded && (
                      <div className="border-t bg-white">
                        {stageTasks.map((task) => (
                          <div key={task.id} className="grid gap-2 border-b px-3 py-3 last:border-b-0 lg:grid-cols-[minmax(240px,2fr)_120px_130px_145px_minmax(170px,1fr)_80px] lg:items-center">
                            <div><div className="flex flex-wrap items-center gap-2"><b className="text-sm">{task.title}</b><span className={`rounded-full px-2 py-0.5 text-[11px] ${priorityStyle(task.priority)}`}>{task.priority}</span>{task.deadline < todayIso() && !isTaskCompleted(task) && <span className="rounded-full bg-rose-100 px-2 py-0.5 text-[11px] text-rose-700">Просрочено</span>}</div>{task.result && <p className="mt-1 text-xs text-slate-500">Результат: {task.result}</p>}</div>
                            <span className="text-sm">{formatDate(task.deadline)}</span>
                            <span className="text-sm font-medium">{task.owner}</span>
                            <select value={task.status} onChange={(event) => updateTaskStatus(task.id, event.target.value)} className={`rounded-xl border-0 px-2 py-1.5 text-sm ${statusStyle(task.status)}`}>{TASK_STATUSES.map((status) => <option key={status}>{status}</option>)}</select>
                            <div className="min-w-0 text-sm text-slate-600"><span className="line-clamp-2">{task.comment || '—'}</span><TaskResourceLink url={task.resource_url} compact /></div>
                            <button type="button" onClick={() => openTaskModal(task)} className="rounded-lg bg-violet-50 p-2 text-violet-700"><Edit3 className="h-4 w-4" /></button>
                          </div>
                        ))}
                        {stageTasks.length === 0 && <div className="p-4 text-center text-sm text-slate-500">По выбранному фильтру задач нет.</div>}
                      </div>
                    )}
                  </div>
                );
              })}

              {tasksWithoutStage.length > 0 && (
                <div className="overflow-hidden rounded-xl border border-dashed border-amber-200 bg-amber-50/40">
                  <div className="px-3 py-2 text-sm font-semibold text-amber-900">Задачи проекта без этапа</div>
                  {tasksWithoutStage.map((task) => <button key={task.id} type="button" onClick={() => openTaskModal(task)} className="flex w-full items-center justify-between border-t bg-white px-3 py-2 text-left text-sm hover:bg-slate-50"><span><b>{task.title}</b><span className="ml-2 text-xs text-slate-500">{task.owner} · {formatDate(task.deadline)}</span></span><span className={`rounded-full px-2 py-0.5 text-[11px] ${statusStyle(task.status)}`}>{task.status}</span></button>)}
                </div>
              )}

              {projectStages.length === 0 && tasksWithoutStage.length === 0 && <div className="rounded-xl border border-dashed p-4 text-center text-sm text-slate-500">В проекте ещё нет этапов и задач.</div>}
            </div>
          )}
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-[radial-gradient(circle_at_top_left,_rgba(124,58,237,0.18),_transparent_32%),radial-gradient(circle_at_top_right,_rgba(14,165,233,0.16),_transparent_28%),linear-gradient(180deg,_#f8fafc_0%,_#eef2ff_45%,_#f8fafc_100%)] p-4 text-slate-900 md:p-8">
      <div className="mx-auto max-w-[1500px] space-y-6">
        <motion.header initial={{ opacity: 0, y: 14 }} animate={{ opacity: 1, y: 0 }} className="overflow-hidden rounded-[2rem] bg-slate-950 px-5 py-6 text-white shadow-[0_24px_70px_rgba(30,41,59,0.25)] md:px-8 md:py-8">
          <div className="flex flex-col gap-6 xl:flex-row xl:items-center xl:justify-between">
            <div className="max-w-3xl">
              <div className="mb-3 inline-flex items-center rounded-full bg-white/10 px-3 py-1 text-xs font-medium text-violet-100 ring-1 ring-white/10">
                <Sparkles className="mr-2 h-3.5 w-3.5" /> MAVIS GROUP · центр проектов
              </div>
              <h1 className="text-3xl font-bold tracking-tight md:text-4xl">Разделы → проекты → этапы → задачи</h1>
              <p className="mt-3 max-w-2xl text-sm leading-6 text-slate-300 md:text-base">Собирайте проекты по направлениям, управляйте стадиями и показывайте только нужные статусы задач без потери общей истории.</p>
              <div className="mt-5 flex flex-wrap gap-2 text-xs text-slate-200">
                <span className="rounded-full bg-cyan-500/20 px-3 py-1.5 ring-1 ring-cyan-400/20">{sections.length} разделов</span><span className="rounded-full bg-violet-500/20 px-3 py-1.5 ring-1 ring-violet-400/20">{projects.length} проектов</span>
                <span className="rounded-full bg-sky-500/20 px-3 py-1.5 ring-1 ring-sky-400/20">{tasks.length} задач</span>
                <span className="rounded-full bg-rose-500/20 px-3 py-1.5 ring-1 ring-rose-400/20">{currentWeekCarryovers.length} переносов с недели</span>
              </div>
            </div>
            <div className="grid gap-2 sm:grid-cols-2 xl:min-w-[900px] xl:grid-cols-6">
              <button type="button" onClick={downloadBackup} className="inline-flex items-center justify-center rounded-2xl bg-white/10 px-4 py-3 text-sm font-medium ring-1 ring-white/15 hover:bg-white/15" title="Скачать резервную копию всех текущих данных">
                <Download className="mr-2 h-5 w-5" /> Резервная копия
              </button>
              <button type="button" onClick={loadData} disabled={loading} className="inline-flex items-center justify-center rounded-2xl bg-white/10 px-4 py-3 text-sm font-medium ring-1 ring-white/15 hover:bg-white/15">
                <RefreshCw className={`mr-2 h-5 w-5 ${loading ? 'animate-spin' : ''}`} /> Обновить
              </button>
              <button type="button" onClick={() => setIsEmployeeModalOpen(true)} className="inline-flex items-center justify-center rounded-2xl bg-sky-500 px-4 py-3 text-sm font-medium hover:bg-sky-400">
                <UserPlus className="mr-2 h-5 w-5" /> Сотрудник
              </button>
              <button type="button" onClick={() => openProjectModal()} className="inline-flex items-center justify-center rounded-2xl bg-emerald-500 px-4 py-3 text-sm font-medium hover:bg-emerald-400">
                <FolderPlus className="mr-2 h-5 w-5" /> Проект
              </button>
              <button type="button" onClick={() => openSectionModal()} className="inline-flex items-center justify-center rounded-2xl bg-cyan-500 px-4 py-3 text-sm font-medium hover:bg-cyan-400">
                <Layers3 className="mr-2 h-5 w-5" /> Раздел
              </button>
              <button type="button" onClick={() => openTaskModal()} className="inline-flex items-center justify-center rounded-2xl bg-violet-500 px-4 py-3 text-sm font-medium hover:bg-violet-400">
                <Plus className="mr-2 h-5 w-5" /> Задача
              </button>
            </div>
          </div>
        </motion.header>

        {message && <div className="flex items-start rounded-2xl border border-indigo-100 bg-indigo-50/95 p-4 text-sm text-indigo-900 shadow-sm"><Database className="mr-2 mt-0.5 h-4 w-4 shrink-0" />{message}</div>}

        {currentWeekCarryovers.length > 0 && activeTab !== 'transfers' && (
          <button type="button" onClick={() => setActiveTab('transfers')} className="flex w-full items-center justify-between rounded-2xl border border-rose-200 bg-gradient-to-r from-rose-50 to-orange-50 p-4 text-left shadow-sm hover:border-rose-300">
            <span className="flex items-start gap-3">
              <span className="rounded-xl bg-rose-100 p-2 text-rose-600"><History className="h-5 w-5" /></span>
              <span><b>С текущей недели перенесено задач: {currentWeekCarryovers.length}</b><span className="mt-1 block text-sm text-slate-600">На следующую неделю — {nextWeekCarryovers.length}. Откройте список, чтобы увидеть старую и новую дату, время переноса и причину.</span></span>
            </span>
            <ArrowRight className="h-5 w-5 text-rose-500" />
          </button>
        )}

        <Card>
          <div className="p-4 md:p-5">
            <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
              <div className="flex flex-wrap gap-2 rounded-2xl bg-slate-100 p-1">
                {[
                  ['sections', 'Структура', Layers3],
                  ['projects', 'Все проекты', FolderKanban],
                  ['calendar', 'Календарь', CalendarDays],
                  ['tasks', 'Все задачи', ListChecks],
                  ['transfers', 'Переносы', History],
                  ['team', 'Команда', Users],
                ].map(([key, label, Icon]) => (
                  <button key={key} type="button" onClick={() => setActiveTab(key)} className={`inline-flex items-center rounded-xl px-4 py-2 text-sm font-medium transition ${activeTab === key ? 'bg-violet-600 text-white shadow-sm' : 'text-slate-600 hover:bg-white'}`}>
                    <Icon className="mr-2 h-4 w-4" />{label}{key === 'transfers' && currentWeekCarryovers.length > 0 && <span className="ml-2 rounded-full bg-white/20 px-2 py-0.5 text-xs">{currentWeekCarryovers.length}</span>}
                  </button>
                ))}
              </div>

              <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
                <div className="relative">
                  <Search className="absolute left-3 top-2.5 h-4 w-4 text-slate-400" />
                  <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Поиск по разделам, проектам и задачам" className="w-full rounded-xl border border-slate-200 bg-white px-3 py-2 pl-9 text-sm sm:w-64" />
                </div>
                <select value={selectedEmployee} onChange={(event) => setSelectedEmployee(event.target.value)} className="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm">
                  <option>Все</option>{employeeNames.map((name) => <option key={name}>{name}</option>)}
                </select>
                <button type="button" onClick={resetAllFilters} className="inline-flex items-center justify-center rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-sm font-medium text-rose-700 hover:bg-rose-100"><X className="mr-1.5 h-4 w-4" />Сбросить все фильтры</button>
              </div>
            </div>

            {['projects', 'sections', 'tasks', 'calendar'].includes(activeTab) && (
              <div className="mt-4 flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
                <div className="flex flex-wrap gap-2">
                  {[
                    ['all', 'Все'],
                    ['active', 'Активные'],
                    ['today', 'На сегодня'],
                    ['overdue', `Просроченные · ${summary.overdue}`],
                  ].map(([key, label]) => (
                    <button key={key} type="button" onClick={() => setTaskFilter(key)} className={`rounded-full px-3 py-1.5 text-xs font-medium ${taskFilter === key ? 'bg-slate-900 text-white' : 'bg-slate-100 text-slate-600 hover:bg-slate-200'}`}>{label}</button>
                  ))}
                </div>
                <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
                  <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)} className="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm">
                    <option value="all">Все статусы</option>
                    <option value="active">Только активные</option>
                    <option value="completed">Только готовые</option>
                    {TASK_STATUSES.map((status) => <option key={status} value={status}>{status}</option>)}
                  </select>
                  <select value={dateFilterMode} onChange={(event) => setDateFilterMode(event.target.value)} className="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm">
                    <option value="all">Все даты</option>
                    <option value="day">Конкретная дата</option>
                    <option value="week">Неделя</option>
                    <option value="month">Месяц</option>
                  </select>
                  {dateFilterMode !== 'all' && (
                    <input
                      type={dateFilterMode === 'month' ? 'month' : 'date'}
                      value={dateFilterMode === 'month' ? dateFilterDate.slice(0, 7) : dateFilterDate}
                      onChange={(event) => { if (event.target.value) setDateFilterDate(dateFilterMode === 'month' ? `${event.target.value}-01` : event.target.value); }}
                      className="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm"
                    />
                  )}
                  <span className="rounded-xl bg-violet-50 px-3 py-2 text-xs font-medium text-violet-700">{dateFilterCaption(dateFilterMode, dateFilterDate)}</span>
                  <button type="button" onClick={resetAllFilters} className="rounded-xl border px-3 py-2 text-xs font-medium text-slate-600 hover:bg-slate-50">Сбросить всё</button>
                </div>
              </div>
            )}
          </div>
        </Card>

        <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6">
          <Metric icon={FolderKanban} label="Активные проекты" value={summary.projects} tone="violet" />
          <Metric icon={ListChecks} label="Всего задач" value={summary.tasks} tone="blue" />
          <Metric icon={Clock} label="Активные" value={summary.active} tone="amber" />
          <Metric icon={CheckCircle2} label="Готово" value={summary.done} tone="emerald" />
          <Metric icon={AlertCircle} label="Просрочено" value={summary.overdue} tone="rose" />
          <Metric icon={History} label="Перенесено с недели" value={summary.carryovers} detail={`${nextWeekCarryovers.length} на следующую`} tone="indigo" />
        </div>

        {activeTab === 'projects' && (
          <div className="space-y-5">
            <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
              <div><h2 className="text-2xl font-bold">Проекты</h2><p className="text-sm text-slate-500">Плоский список всех проектов. Раздел можно изменить прямо в карточке проекта.</p></div>
              <div className="flex flex-wrap gap-2">
                <label className="inline-flex cursor-pointer items-center rounded-2xl border border-sky-200 bg-sky-50 px-4 py-2.5 text-sm font-medium text-sky-700 hover:bg-sky-100"><FileUp className="mr-2 h-4 w-4" />Импорт таблицы<input type="file" accept=".xlsx,.xls" className="hidden" onChange={importProjectTable} /></label>
                <button type="button" onClick={() => openProjectModal()} className="inline-flex items-center rounded-2xl bg-emerald-600 px-4 py-2.5 text-sm font-medium text-white hover:bg-emerald-500"><FolderPlus className="mr-2 h-4 w-4" />Новый проект</button>
              </div>
            </div>

            {filteredProjects.map((project) => {
              const projectStages = stages.filter((stage) => String(stage.project_id) === String(project.id)).sort((a, b) => a.sort_order - b.sort_order);
              const projectTasks = filteredTasks.filter((task) => String(task.project_id) === String(project.id));
              const allProjectTasks = tasks.filter((task) => String(task.project_id) === String(project.id));
              const progress = calculateProgress(allProjectTasks);
              const derivedStatus = deriveStatus(allProjectTasks, project.status);
              const expanded = expandedProjects[project.id] !== false;
              return (
                <Card key={project.id} className="overflow-hidden">
                  <div className="h-2" style={{ backgroundColor: project.color }} />
                  <div className="p-5 md:p-6">
                    <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
                      <button type="button" onClick={() => setExpandedProjects((previous) => ({ ...previous, [project.id]: !expanded }))} className="flex flex-1 items-start gap-3 text-left">
                        <span className="mt-1 rounded-2xl p-2.5 text-white" style={{ backgroundColor: project.color }}><FolderKanban className="h-5 w-5" /></span>
                        <span className="min-w-0 flex-1">
                          <span className="flex flex-wrap items-center gap-2"><span className="text-xl font-bold">{project.name}</span><span className={`rounded-full px-3 py-1 text-xs ${statusStyle(derivedStatus)}`}>{derivedStatus}</span></span>
                          <span className="mt-1 block text-sm text-slate-500">{project.description || 'Описание проекта не заполнено'}</span>
                          <span className="mt-3 block max-w-xl"><ProgressBar value={progress} color={project.color} /></span>
                        </span>
                        {expanded ? <ChevronUp className="mt-2 h-5 w-5 text-slate-400" /> : <ChevronDown className="mt-2 h-5 w-5 text-slate-400" />}
                      </button>
                      <div className="flex flex-wrap gap-2 text-sm">
                        <span className="rounded-xl bg-slate-100 px-3 py-2"><b>Ответственный: {project.owner}</b> · {project.deadline ? formatDate(project.deadline) : 'без общего срока'}</span><select value={project.section_id || ''} onChange={(event) => moveProjectToSection(project, event.target.value)} className="rounded-xl border border-cyan-200 bg-cyan-50 px-3 py-2 text-sm font-medium text-cyan-800"><option value="">Без раздела</option>{sections.map((section) => <option key={section.id} value={section.id}>{section.name}</option>)}</select>{project.customer && <span className="rounded-xl bg-amber-50 px-3 py-2 text-amber-800"><b>Заказчик:</b> {project.customer}</span>}
                        <span className="rounded-xl bg-slate-100 px-3 py-2">{projectStages.length} этапов · {allProjectTasks.length} задач</span>
                        <button type="button" onClick={() => openStageModal(project.id)} className="inline-flex items-center rounded-xl bg-sky-50 px-3 py-2 font-medium text-sky-700 hover:bg-sky-100"><Layers3 className="mr-1.5 h-4 w-4" />Этап</button>
                        <button type="button" onClick={() => openTaskModal(null, project.id)} className="inline-flex items-center rounded-xl bg-violet-50 px-3 py-2 font-medium text-violet-700 hover:bg-violet-100"><Plus className="mr-1.5 h-4 w-4" />Задача</button>
                        <button type="button" onClick={() => openProjectModal(project)} className="rounded-xl border px-3 py-2 text-slate-600 hover:bg-slate-50"><Edit3 className="h-4 w-4" /></button>
                        <button type="button" onClick={() => deleteProject(project)} className="rounded-xl border border-rose-100 px-3 py-2 text-rose-600 hover:bg-rose-50"><Trash2 className="h-4 w-4" /></button>
                      </div>
                    </div>

                    {expanded && (
                      <div className="mt-6 space-y-3">
                        {projectStages.map((stage) => {
                          const allStageTasks = tasks.filter((task) => String(task.stage_id) === String(stage.id));
                          const stageTasks = projectTasks.filter((task) => String(task.stage_id) === String(stage.id));
                          const stageProgress = calculateProgress(allStageTasks);
                          const stageStatus = deriveStatus(allStageTasks);
                          const stageExpanded = expandedStages[stage.id] !== false;
                          return (
                            <div key={stage.id} className="overflow-hidden rounded-2xl border border-slate-200 bg-slate-50/80">
                              <div className="flex flex-col gap-3 p-4 lg:flex-row lg:items-center lg:justify-between">
                                <button type="button" onClick={() => setExpandedStages((previous) => ({ ...previous, [stage.id]: !stageExpanded }))} className="flex flex-1 items-center gap-3 text-left">
                                  <span className="rounded-xl bg-white p-2 text-violet-600 shadow-sm"><Layers3 className="h-4 w-4" /></span>
                                  <span className="min-w-0 flex-1">
                                    <span className="flex flex-wrap items-center gap-2"><b>{stage.sort_order}. {stage.title}</b><span className={`rounded-full px-2.5 py-1 text-xs ${statusStyle(stageStatus)}`}>{stageStatus}</span></span>
                                    <span className="mt-1 block max-w-md"><ProgressBar value={stageProgress} color={project.color} /></span>
                                  </span>
                                  {stageExpanded ? <ChevronUp className="h-4 w-4 text-slate-400" /> : <ChevronDown className="h-4 w-4 text-slate-400" />}
                                </button>
                                <div className="flex flex-wrap items-center gap-2 text-xs text-slate-600">
                                  <span className="rounded-lg bg-white px-2.5 py-1.5">{stage.owner}</span>
                                  <span className="rounded-lg bg-white px-2.5 py-1.5">{stage.deadline ? formatDate(stage.deadline) : 'Без срока'}</span>
                                  <span className="rounded-lg bg-white px-2.5 py-1.5">{allStageTasks.length} задач</span>
                                  <button type="button" onClick={() => openTaskModal(null, project.id, stage.id)} className="inline-flex items-center rounded-lg bg-violet-600 px-3 py-1.5 font-medium text-white hover:bg-violet-500"><Plus className="mr-1 h-3.5 w-3.5" />Задача</button>
                                  <button type="button" onClick={() => openStageModal(project.id, stage)} className="rounded-lg bg-white p-1.5 hover:bg-slate-100"><Edit3 className="h-3.5 w-3.5" /></button>
                                  <button type="button" onClick={() => deleteStage(stage)} className="rounded-lg bg-white p-1.5 text-rose-600 hover:bg-rose-50"><Trash2 className="h-3.5 w-3.5" /></button>
                                </div>
                              </div>

                              {stageExpanded && (
                                <div className="border-t bg-white">
                                  <div className="hidden grid-cols-[minmax(260px,2fr)_130px_135px_150px_minmax(180px,1fr)_110px] gap-3 border-b bg-slate-50 px-4 py-2 text-xs font-semibold uppercase tracking-wide text-slate-500 lg:grid">
                                    <span>Задача</span><span>Дедлайн</span><span>Ответственный</span><span>Статус</span><span>Комментарий</span><span>Действия</span>
                                  </div>
                                  {stageTasks.map((task) => (
                                    <div key={task.id} className="grid gap-3 border-b px-4 py-3 last:border-b-0 lg:grid-cols-[minmax(260px,2fr)_130px_135px_150px_minmax(180px,1fr)_110px] lg:items-center">
                                      <div><div className="flex flex-wrap items-center gap-2"><b className="text-sm">{task.title}</b><span className={`rounded-full px-2 py-0.5 text-[11px] ${priorityStyle(task.priority)}`}>{task.priority}</span>{task.deadline < todayIso() && task.status !== 'Готово' && <span className="rounded-full bg-rose-100 px-2 py-0.5 text-[11px] text-rose-700">Просрочено</span>}</div>{task.result && <p className="mt-1 text-xs text-slate-500">Результат: {task.result}</p>}</div>
                                      <span className="text-sm">{formatDate(task.deadline)}</span>
                                      <span className="text-sm font-medium">{task.owner}</span>
                                      <select value={task.status} onChange={(event) => updateTaskStatus(task.id, event.target.value)} className={`rounded-xl border-0 px-2.5 py-2 text-sm ${statusStyle(task.status)}`}>{TASK_STATUSES.map((status) => <option key={status}>{status}</option>)}</select>
                                      <div className="min-w-0 text-sm text-slate-600"><span className="block">{task.comment || '—'}</span><TaskResourceLink url={task.resource_url} compact /></div>
                                      <span className="flex gap-1"><button type="button" onClick={() => openTaskModal(task)} className="rounded-lg bg-violet-50 p-2 text-violet-700 hover:bg-violet-100"><Edit3 className="h-4 w-4" /></button><button type="button" onClick={() => deleteTask(task)} className="rounded-lg bg-rose-50 p-2 text-rose-600 hover:bg-rose-100"><Trash2 className="h-4 w-4" /></button></span>
                                    </div>
                                  ))}
                                  {stageTasks.length === 0 && <div className="p-5 text-center text-sm text-slate-500">В этом этапе пока нет задач.</div>}
                                </div>
                              )}
                            </div>
                          );
                        })}
                        {projectStages.length === 0 && <div className="rounded-2xl border border-dashed bg-slate-50 p-6 text-center text-sm text-slate-500">В проекте ещё нет этапов. Нажмите «Этап», чтобы создать первый блок работ.</div>}
                      </div>
                    )}
                  </div>
                </Card>
              );
            })}

            {projects.length === 0 && <Card><div className="p-10 text-center"><FolderKanban className="mx-auto h-10 w-10 text-violet-400" /><h3 className="mt-3 text-lg font-semibold">Создайте первый проект</h3><p className="mt-1 text-sm text-slate-500">Затем добавьте этапы и задачи с ответственными, дедлайнами, статусами и комментариями.</p><button type="button" onClick={() => openProjectModal()} className="mt-4 rounded-2xl bg-violet-600 px-5 py-3 text-sm font-medium text-white">Создать проект</button></div></Card>}

            {filteredTasks.filter((task) => !task.project_id && !task.section_id).length > 0 && (
              <Card>
                <div className="p-5"><h3 className="text-lg font-semibold">Задачи без проекта и раздела</h3><p className="text-sm text-slate-500">Их можно открыть и привязать к проекту, этапу или разделу.</p><div className="mt-4 space-y-2">{filteredTasks.filter((task) => !task.project_id && !task.section_id).map((task) => <button key={task.id} type="button" onClick={() => openTaskModal(task)} className="flex w-full items-center justify-between rounded-xl border p-3 text-left hover:border-violet-200"><span><b>{task.title}</b><span className="mt-1 block text-xs text-slate-500">{task.owner} · {formatDate(task.deadline)}</span></span><span className={`rounded-full px-3 py-1 text-xs ${statusStyle(task.status)}`}>{task.status}</span></button>)}</div></div>
              </Card>
            )}
          </div>
        )}

        {activeTab === 'sections' && (
          <div className="space-y-5">
            <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
              <div><h2 className="text-2xl font-bold">Структура работы</h2><p className="text-sm text-slate-500">Иерархия: раздел → проекты → этапы → задачи. В каждом разделе можно отдельно выбрать, какие статусы задач показывать.</p></div>
              <div className="flex flex-wrap gap-2"><button type="button" onClick={() => openSectionModal()} className="inline-flex items-center justify-center rounded-2xl bg-cyan-600 px-4 py-2.5 text-sm font-medium text-white hover:bg-cyan-500"><Layers3 className="mr-2 h-4 w-4" />Новый раздел</button><button type="button" onClick={() => openProjectModal()} className="inline-flex items-center justify-center rounded-2xl bg-emerald-600 px-4 py-2.5 text-sm font-medium text-white hover:bg-emerald-500"><FolderPlus className="mr-2 h-4 w-4" />Новый проект</button></div>
            </div>

            {filteredSections.map((section) => {
              const sectionProjects = filteredProjects.filter((project) => String(project.section_id) === String(section.id));
              const allSectionProjects = projects.filter((project) => String(project.section_id) === String(section.id));
              const projectIds = new Set(allSectionProjects.map((project) => String(project.id)));
              const allSectionTasks = tasks.filter((task) => projectIds.has(String(task.project_id)) || (!task.project_id && String(task.section_id) === String(section.id)));
              const directSectionTasks = filteredTasks.filter((task) => !task.project_id && String(task.section_id) === String(section.id));
              const localStatusMode = sectionStatusFilters[section.id] || 'active';
              const visibleDirectTasks = directSectionTasks.filter((task) => matchesLocalTaskStatus(task, localStatusMode));
              const progress = calculateProgress(allSectionTasks);
              const sectionExpanded = expandedSections[section.id] !== false;
              return (
                <Card key={section.id} className="overflow-hidden">
                  <div className="h-2" style={{ backgroundColor: section.color }} />
                  <div className="p-5 md:p-6">
                    <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
                      <button type="button" onClick={() => setExpandedSections((previous) => ({ ...previous, [section.id]: !sectionExpanded }))} className="flex min-w-0 flex-1 items-start gap-3 text-left">
                        <span className="rounded-2xl p-2.5 text-white" style={{ backgroundColor: section.color }}><Layers3 className="h-5 w-5" /></span>
                        <span className="min-w-0 flex-1"><span className="flex flex-wrap items-center gap-2"><h3 className="text-xl font-bold">{section.name}</h3><span className="rounded-full bg-cyan-50 px-3 py-1 text-xs font-medium text-cyan-800">{allSectionProjects.length} проектов</span></span><span className="mt-1 block text-sm text-slate-500">{section.description || 'Описание раздела не заполнено'}</span><span className="mt-3 block max-w-xl"><ProgressBar value={progress} color={section.color} /></span></span>
                        {sectionExpanded ? <ChevronUp className="mt-2 h-5 w-5 text-slate-400" /> : <ChevronDown className="mt-2 h-5 w-5 text-slate-400" />}
                      </button>
                      <div className="flex flex-wrap items-center gap-2 text-sm">
                        <span className="rounded-xl bg-slate-100 px-3 py-2"><b>Ответственный:</b> {section.owner}</span>
                        <span className="rounded-xl bg-slate-100 px-3 py-2">{allSectionTasks.filter((task) => !isTaskCompleted(task)).length} активных · {allSectionTasks.length} всего</span>
                        <select value={localStatusMode} onChange={(event) => setSectionStatusFilters((previous) => ({ ...previous, [section.id]: event.target.value }))} className="rounded-xl border border-violet-200 bg-violet-50 px-3 py-2 text-sm font-medium text-violet-800"><option value="active">Активные задачи</option><option value="all">Все задачи</option><option value="completed">Только готовые</option>{TASK_STATUSES.map((status) => <option key={status} value={status}>{status}</option>)}</select>
                        <button type="button" onClick={() => openProjectModal(null, section.id)} className="inline-flex items-center rounded-xl bg-emerald-50 px-3 py-2 font-medium text-emerald-700"><FolderPlus className="mr-1.5 h-4 w-4" />Проект</button>
                        <button type="button" onClick={() => openSectionModal(section)} className="rounded-xl border p-2 text-slate-600"><Edit3 className="h-4 w-4" /></button>
                        <button type="button" onClick={() => deleteSection(section)} className="rounded-xl border border-rose-100 p-2 text-rose-600"><Trash2 className="h-4 w-4" /></button>
                      </div>
                    </div>

                    {sectionExpanded && (
                      <div className="mt-6 space-y-4">
                        {sectionProjects.map((project) => renderHierarchyProject(project, localStatusMode))}
                        {sectionProjects.length === 0 && <div className="rounded-2xl border border-dashed bg-slate-50 p-6 text-center text-sm text-slate-500">В этом разделе нет проектов по выбранным общим фильтрам. Создайте проект или выберите раздел в карточке существующего проекта.</div>}

                        {visibleDirectTasks.length > 0 && (
                          <div className="rounded-2xl border border-dashed border-cyan-200 bg-cyan-50/40 p-4">
                            <div className="mb-3 flex items-center justify-between"><div><b>Отдельные задачи без проекта</b><p className="text-xs text-slate-500">Сохранены для совместимости. Для новых крупных работ лучше создавать проект.</p></div><button type="button" onClick={() => openTaskModal(null, '', '', section.id)} className="rounded-xl bg-violet-600 px-3 py-2 text-xs font-medium text-white">+ Задача</button></div>
                            <div className="space-y-2">{visibleDirectTasks.map((task) => <button key={task.id} type="button" onClick={() => openTaskModal(task)} className="flex w-full items-center justify-between rounded-xl border bg-white p-3 text-left hover:border-violet-200"><span><b>{task.title}</b><span className="mt-1 block text-xs text-slate-500">{task.owner} · {formatDate(task.deadline)}</span></span><span className={`rounded-full px-2 py-0.5 text-[11px] ${statusStyle(task.status)}`}>{task.status}</span></button>)}</div>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                </Card>
              );
            })}

            {filteredProjects.filter((project) => !project.section_id).length > 0 && (
              <Card className="overflow-hidden border-amber-200">
                <div className="h-2 bg-amber-500" />
                <div className="p-5 md:p-6">
                  <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between"><div><h3 className="text-xl font-bold">Проекты без раздела</h3><p className="text-sm text-slate-500">Выберите раздел в выпадающем списке проекта — перенос сохранится без изменения этапов и задач.</p></div><span className="rounded-xl bg-amber-50 px-3 py-2 text-sm font-medium text-amber-800">{filteredProjects.filter((project) => !project.section_id).length} проектов</span></div>
                  <div className="mt-5 space-y-4">{filteredProjects.filter((project) => !project.section_id).map((project) => renderHierarchyProject(project, sectionStatusFilters.unassigned || 'active'))}</div>
                </div>
              </Card>
            )}

            {sections.length === 0 && <Card><div className="p-10 text-center"><Layers3 className="mx-auto h-10 w-10 text-cyan-500" /><h3 className="mt-3 text-lg font-semibold">Создайте первый раздел</h3><p className="mt-1 text-sm text-slate-500">Например: «Продажи», «Экспертный отдел», «Маркетинг», «Автоматизация» или «Финансы».</p><button type="button" onClick={() => openSectionModal()} className="mt-4 rounded-2xl bg-cyan-600 px-5 py-3 text-sm font-medium text-white">Создать раздел</button></div></Card>}
          </div>
        )}

        {activeTab === 'calendar' && (
          <div className="space-y-4">
            <Card><div className="flex flex-col gap-3 p-4 sm:flex-row sm:items-center sm:justify-between"><div className="flex flex-wrap items-center gap-2"><button type="button" onClick={() => setSelectedDate(addDays(selectedDate, -7))} className="rounded-xl border bg-white p-2"><ChevronLeft className="h-5 w-5" /></button><input type="date" value={selectedDate} onChange={(event) => setSelectedDate(event.target.value)} className="rounded-xl border px-3 py-2 text-sm" /><button type="button" onClick={() => setSelectedDate(addDays(selectedDate, 7))} className="rounded-xl border bg-white p-2"><ChevronRight className="h-5 w-5" /></button><button type="button" onClick={() => setSelectedDate(todayIso())} className="rounded-xl bg-slate-900 px-3 py-2 text-sm font-medium text-white">Сегодня</button><span className="inline-flex items-center rounded-xl bg-emerald-50 px-3 py-2 text-xs font-medium text-emerald-700"><ShieldCheck className="mr-1.5 h-4 w-4" />Перетаскивание не удаляет задачи</span></div><button type="button" onClick={() => openTaskModal()} className="inline-flex items-center justify-center rounded-xl bg-violet-600 px-4 py-2 text-sm font-medium text-white"><Plus className="mr-2 h-4 w-4" />Новая задача</button></div></Card>
            <WeekCalendar tasks={calendarTasks} unscheduledTasks={unscheduledCalendarTasks} selectedDate={selectedDate} onEditTask={openTaskModal} onMoveTask={moveTaskInCalendar} />
          </div>
        )}

        {activeTab === 'tasks' && (
          <Card>
            <div className="p-5">
              <div className="mb-5 flex flex-col gap-3 md:flex-row md:items-center md:justify-between"><div><h2 className="text-xl font-semibold">Все задачи</h2><p className="text-sm text-slate-500">Общий список с проектом, этапом, сроком и последним комментарием.</p></div><button type="button" onClick={() => openTaskModal()} className="inline-flex items-center justify-center rounded-2xl bg-violet-600 px-5 py-3 text-sm font-medium text-white"><Plus className="mr-2 h-4 w-4" />Новая задача</button></div>
              <div className="space-y-3">{filteredTasks.map((task) => {
                const project = projectById.get(String(task.project_id));
                const stage = stageById.get(String(task.stage_id));
                return <div key={task.id} className="rounded-2xl border bg-white p-4 hover:border-violet-200 hover:shadow-sm"><div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between"><div className="space-y-2"><div className="flex flex-wrap gap-2"><span className={`rounded-full px-3 py-1 text-xs ${statusStyle(task.status)}`}>{task.status}</span><span className={`rounded-full px-3 py-1 text-xs ${priorityStyle(task.priority)}`}>{task.priority}</span>{project && <span className="rounded-full bg-violet-50 px-3 py-1 text-xs text-violet-700">{project.name}</span>}{stage && <span className="rounded-full bg-sky-50 px-3 py-1 text-xs text-sky-700">{stage.title}</span>}{sectionById.get(String(project?.section_id || task.section_id)) && <span className="rounded-full bg-cyan-50 px-3 py-1 text-xs text-cyan-700">{sectionById.get(String(project?.section_id || task.section_id)).name}</span>}</div><h3 className="text-lg font-semibold">{task.title}</h3><p className="text-sm text-slate-600"><b>Комментарий:</b> {task.comment || '—'}</p>{task.resource_url && <TaskResourceLink url={task.resource_url} />}<p className="text-sm text-slate-500">{task.owner} · {formatDate(task.deadline)} · {formatTime(task.start_time)}{task.end_time ? `–${formatTime(task.end_time)}` : ''} · {task.hours} ч</p></div><div className="flex flex-wrap gap-2"><select value={task.status} onChange={(event) => updateTaskStatus(task.id, event.target.value)} className={`rounded-xl border-0 px-3 py-2 text-sm ${statusStyle(task.status)}`}>{TASK_STATUSES.map((status) => <option key={status}>{status}</option>)}</select><button type="button" onClick={() => openTaskModal(task)} className="inline-flex items-center rounded-xl bg-violet-50 px-3 py-2 text-sm font-medium text-violet-700"><Edit3 className="mr-2 h-4 w-4" />Изменить</button><button type="button" onClick={() => deleteTask(task)} className="rounded-xl border border-rose-100 px-3 py-2 text-rose-600"><Trash2 className="h-4 w-4" /></button></div></div></div>;
              })}{filteredTasks.length === 0 && <div className="rounded-2xl border border-dashed p-8 text-center text-slate-500">По выбранным фильтрам задач нет.</div>}</div>
            </div>
          </Card>
        )}

        {activeTab === 'transfers' && (
          <div className="space-y-5">
            <Card>
              <div className="p-5">
                <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between"><div><h2 className="flex items-center text-2xl font-bold"><History className="mr-2 h-6 w-6 text-rose-500" />История переносов</h2><p className="mt-1 text-sm text-slate-500">Каждое изменение дедлайна сохраняется отдельно: откуда, куда, когда, кто и почему перенёс.</p></div><div className="flex flex-wrap rounded-xl bg-slate-100 p-1">{[['current', 'С текущей недели'], ['next', 'На следующую'], ['all', 'Вся история']].map(([key, label]) => <button key={key} type="button" onClick={() => setTransferFilter(key)} className={`rounded-lg px-3 py-2 text-sm font-medium ${transferFilter === key ? 'bg-white text-rose-700 shadow-sm' : 'text-slate-600'}`}>{label}</button>)}</div></div>
                <div className="mt-5 grid gap-3 md:grid-cols-3"><Metric icon={CalendarClock} label="С текущей недели" value={currentWeekCarryovers.length} detail={`${formatDate(currentWeekStart)} — ${formatDate(currentWeekEnd)}`} tone="rose" /><Metric icon={ArrowRight} label="На следующую неделю" value={nextWeekCarryovers.length} detail={`${formatDate(nextWeekStart)} — ${formatDate(nextWeekEnd)}`} tone="amber" /><Metric icon={History} label="Всего изменений сроков" value={reschedules.length} tone="indigo" /></div>
              </div>
            </Card>

            <Card>
              <div className="overflow-x-auto p-5">
                <div className="min-w-[1050px]">
                  <div className="grid grid-cols-[minmax(230px,2fr)_170px_135px_32px_135px_170px_140px_minmax(180px,1fr)] gap-3 border-b bg-slate-50 px-4 py-3 text-xs font-semibold uppercase tracking-wide text-slate-500"><span>Задача / проект</span><span>Ответственный</span><span>Было</span><span></span><span>Стало</span><span>Когда перенесли</span><span>Кто перенёс</span><span>Причина</span></div>
                  {displayedTransfers.map((item) => {
                    const task = taskById.get(String(item.task_id));
                    const project = projectById.get(String(item.project_id));
                    const stage = stageById.get(String(item.stage_id));
                    return <div key={item.id} className="grid grid-cols-[minmax(230px,2fr)_170px_135px_32px_135px_170px_140px_minmax(180px,1fr)] gap-3 border-b px-4 py-4 text-sm last:border-b-0"><span><b>{item.task_title}</b><span className="mt-1 block text-xs text-slate-500">{project?.name || 'Без проекта'}{stage ? ` · ${stage.title}` : ''}</span></span><span>{task?.owner || '—'}<span className={`mt-1 block w-fit rounded-full px-2 py-0.5 text-[11px] ${statusStyle(task?.status || 'Ожидает')}`}>{task?.status || 'Задача удалена'}</span></span><span className="font-medium text-rose-700">{formatDate(item.old_deadline)}</span><ArrowRight className="h-4 w-4 text-slate-400" /><span className="font-medium text-emerald-700">{formatDate(item.new_deadline)}</span><span>{formatDateTime(item.changed_at)}</span><span className="font-medium">{item.changed_by}</span><span className="text-slate-600">{item.reason || 'Причина не указана'}</span></div>;
                  })}
                  {displayedTransfers.length === 0 && <div className="p-10 text-center text-slate-500">В выбранном периоде переносов нет.</div>}
                </div>
              </div>
            </Card>
          </div>
        )}

        {activeTab === 'team' && (
          <div className="grid gap-5 xl:grid-cols-[1.1fr_1fr]">
            <Card><div className="p-5"><div className="mb-5 flex items-center justify-between"><div><h2 className="text-xl font-semibold">Команда</h2><p className="text-sm text-slate-500">Главный показатель — количество активных задач у сотрудника.</p></div><button type="button" onClick={() => setIsEmployeeModalOpen(true)} className="inline-flex items-center rounded-xl bg-sky-600 px-4 py-2 text-sm font-medium text-white"><UserPlus className="mr-2 h-4 w-4" />Добавить</button></div><div className="grid gap-3 sm:grid-cols-2">{employees.map((employee) => { const allPersonTasks = tasks.filter((task) => task.owner === employee.name); const activePersonTasks = allPersonTasks.filter((task) => !isTaskCompleted(task)); const overduePersonTasks = activePersonTasks.filter((task) => task.deadline < todayIso()); return <div key={employee.id} className="rounded-2xl border p-4"><div className="flex items-start justify-between gap-3"><div className="flex items-center gap-3"><span className="flex h-11 w-11 items-center justify-center rounded-2xl text-sm font-bold text-white" style={{ backgroundColor: employee.color }}>{employeeInitials(employee.name)}</span><div><b>{employee.name}</b><p className="text-sm text-slate-500">{employee.role}</p></div></div><button type="button" onClick={() => deleteEmployee(employee)} className="rounded-lg p-2 text-rose-500 hover:bg-rose-50" title="Удалить сотрудника"><Trash2 className="h-4 w-4" /></button></div><div className="mt-4 grid grid-cols-3 gap-2 text-center text-xs"><span className="rounded-lg bg-violet-50 px-2 py-2 text-violet-800"><b className="block text-lg">{activePersonTasks.length}</b>активных</span><span className="rounded-lg bg-rose-50 px-2 py-2 text-rose-700"><b className="block text-lg">{overduePersonTasks.length}</b>просрочено</span><span className="rounded-lg bg-slate-100 px-2 py-2 text-slate-700"><b className="block text-lg">{allPersonTasks.length}</b>всего</span></div></div>; })}</div></div></Card>
            <Card><div className="p-5"><h2 className="flex items-center text-xl font-semibold"><BarChart3 className="mr-2 h-5 w-5" />Загрузка по количеству задач</h2><p className="mb-4 text-sm text-slate-500">Количество незавершённых задач у каждого сотрудника. Часы в расчёте не используются.</p><div className="h-[360px]"><ResponsiveContainer width="100%" height="100%"><BarChart data={workload} layout="vertical" margin={{ left: 15, right: 25 }}><XAxis type="number" allowDecimals={false} /><YAxis dataKey="name" type="category" width={90} /><Tooltip formatter={(value, name, props) => [`${value} активных · ${props.payload.overdue} просрочено`, 'Загрузка']} /><Bar dataKey="tasks" radius={[0, 10, 10, 0]}>{workload.map((entry) => <Cell key={entry.name} fill={entry.color} />)}</Bar></BarChart></ResponsiveContainer></div></div></Card>
          </div>
        )}
      </div>

      {isProjectModalOpen && (
        <Modal title={editingProjectId ? 'Редактирование проекта' : 'Новый проект'} subtitle="Общий контейнер для этапов и задач." onClose={() => setIsProjectModalOpen(false)} maxWidth="max-w-2xl">
          <div className="grid gap-4 md:grid-cols-2">
            <Field label="Название проекта" className="md:col-span-2"><input value={projectForm.name} onChange={(event) => setProjectForm({ ...projectForm, name: event.target.value })} placeholder="Например: ИИ-ассистент экспертного отдела" className="w-full rounded-2xl border px-3 py-2.5" /></Field>
            <Field label="Раздел"><select value={projectForm.section_id} onChange={(event) => setProjectForm({ ...projectForm, section_id: event.target.value })} className="w-full rounded-2xl border bg-white px-3 py-2.5"><option value="">Без раздела</option>{sections.map((section) => <option key={section.id} value={section.id}>{section.name}</option>)}</select></Field>
            <Field label="Ответственный за проект"><select value={projectForm.owner} onChange={(event) => setProjectForm({ ...projectForm, owner: event.target.value })} className="w-full rounded-2xl border bg-white px-3 py-2.5">{employeeNames.map((name) => <option key={name}>{name}</option>)}</select></Field>
            <Field label="Заказчик проекта"><input value={projectForm.customer} onChange={(event) => setProjectForm({ ...projectForm, customer: event.target.value })} placeholder="Например: Виктория, отдел продаж или внешний клиент" className="w-full rounded-2xl border px-3 py-2.5" /></Field>
            <Field label="Общий дедлайн"><input type="date" value={projectForm.deadline} onChange={(event) => setProjectForm({ ...projectForm, deadline: event.target.value })} className="w-full rounded-2xl border px-3 py-2.5" /></Field>
            <Field label="Статус"><select value={projectForm.status} onChange={(event) => setProjectForm({ ...projectForm, status: event.target.value })} className="w-full rounded-2xl border bg-white px-3 py-2.5">{PROJECT_STATUSES.map((status) => <option key={status}>{status}</option>)}</select></Field>
            <Field label="Цвет проекта"><div className="flex flex-wrap gap-2">{PROJECT_COLORS.map((color) => <button key={color} type="button" onClick={() => setProjectForm({ ...projectForm, color })} className={`h-9 w-9 rounded-full ${projectForm.color === color ? 'ring-2 ring-slate-900 ring-offset-2' : ''}`} style={{ backgroundColor: color }} />)}</div></Field>
            <Field label="Описание" className="md:col-span-2"><textarea value={projectForm.description} onChange={(event) => setProjectForm({ ...projectForm, description: event.target.value })} rows="4" placeholder="Цель проекта, ожидаемый результат, важные ограничения" className="w-full rounded-2xl border px-3 py-2.5" /></Field>
          </div>
          <ModalActions onCancel={() => setIsProjectModalOpen(false)} onSave={saveProject} saveLabel={editingProjectId ? 'Сохранить проект' : 'Создать проект'} />
        </Modal>
      )}

      {isStageModalOpen && (
        <Modal title={editingStageId ? 'Редактирование этапа' : 'Новый этап'} subtitle="Крупный блок работ внутри проекта." onClose={() => setIsStageModalOpen(false)} maxWidth="max-w-2xl">
          <div className="grid gap-4 md:grid-cols-2">
            <Field label="Проект" className="md:col-span-2"><select value={stageForm.project_id} onChange={(event) => setStageForm({ ...stageForm, project_id: event.target.value })} className="w-full rounded-2xl border bg-white px-3 py-2.5">{projects.map((project) => <option key={project.id} value={project.id}>{project.name}</option>)}</select></Field>
            <Field label="Название этапа" className="md:col-span-2"><input value={stageForm.title} onChange={(event) => setStageForm({ ...stageForm, title: event.target.value })} placeholder="Например: Финальная настройка" className="w-full rounded-2xl border px-3 py-2.5" /></Field>
            <Field label="Ответственный"><select value={stageForm.owner} onChange={(event) => setStageForm({ ...stageForm, owner: event.target.value })} className="w-full rounded-2xl border bg-white px-3 py-2.5">{employeeNames.map((name) => <option key={name}>{name}</option>)}</select></Field>
            <Field label="Дедлайн этапа"><input type="date" value={stageForm.deadline} onChange={(event) => setStageForm({ ...stageForm, deadline: event.target.value })} className="w-full rounded-2xl border px-3 py-2.5" /></Field>
            <Field label="Порядок"><input type="number" min="1" value={stageForm.sort_order} onChange={(event) => setStageForm({ ...stageForm, sort_order: event.target.value })} className="w-full rounded-2xl border px-3 py-2.5" /></Field>
            <Field label="Описание этапа" className="md:col-span-2"><textarea value={stageForm.description} onChange={(event) => setStageForm({ ...stageForm, description: event.target.value })} rows="3" className="w-full rounded-2xl border px-3 py-2.5" /></Field>
          </div>
          <ModalActions onCancel={() => setIsStageModalOpen(false)} onSave={saveStage} saveLabel={editingStageId ? 'Сохранить этап' : 'Добавить этап'} />
        </Modal>
      )}

      {isSectionModalOpen && (
        <Modal title={editingSectionId ? 'Редактирование раздела' : 'Новый раздел'} subtitle="Раздел объединяет проекты одного направления." onClose={() => setIsSectionModalOpen(false)} maxWidth="max-w-2xl">
          <div className="grid gap-4 md:grid-cols-2">
            <Field label="Название раздела" className="md:col-span-2"><input value={sectionForm.name} onChange={(event) => setSectionForm({ ...sectionForm, name: event.target.value })} placeholder="Например: Продажи, Производство, Маркетинг или Автоматизация" className="w-full rounded-2xl border px-3 py-2.5" /></Field>
            <Field label="Ответственный за раздел"><select value={sectionForm.owner} onChange={(event) => setSectionForm({ ...sectionForm, owner: event.target.value })} className="w-full rounded-2xl border bg-white px-3 py-2.5">{employeeNames.map((name) => <option key={name}>{name}</option>)}</select></Field>
            <Field label="Цвет раздела"><div className="flex flex-wrap gap-2">{SECTION_COLORS.map((color) => <button key={color} type="button" onClick={() => setSectionForm({ ...sectionForm, color })} className={`h-9 w-9 rounded-full ${sectionForm.color === color ? 'ring-2 ring-slate-900 ring-offset-2' : ''}`} style={{ backgroundColor: color }} />)}</div></Field>
            <Field label="Описание" className="md:col-span-2"><textarea value={sectionForm.description} onChange={(event) => setSectionForm({ ...sectionForm, description: event.target.value })} rows="3" placeholder="Какие проекты и процессы относятся к этому разделу" className="w-full rounded-2xl border px-3 py-2.5" /></Field>
          </div>
          <ModalActions onCancel={() => setIsSectionModalOpen(false)} onSave={saveSection} saveLabel={editingSectionId ? 'Сохранить раздел' : 'Создать раздел'} />
        </Modal>
      )}

      {isTaskModalOpen && (
        <Modal title={editingTaskId ? 'Редактирование задачи' : 'Новая задача'} subtitle="Основная структура: раздел → проект → этап → задача. Для старых операционных задач сохранена прямая привязка к разделу." onClose={() => setIsTaskModalOpen(false)} maxWidth="max-w-3xl">
          <div className="grid gap-4 md:grid-cols-2">
            <Field label="Задача" className="md:col-span-2"><input value={taskForm.title} onChange={(event) => setTaskForm({ ...taskForm, title: event.target.value })} placeholder="Конкретное действие и ожидаемый результат" className="w-full rounded-2xl border px-3 py-2.5" /></Field>
            <Field label="Проект"><select value={taskForm.project_id} onChange={(event) => setTaskForm({ ...taskForm, project_id: event.target.value, stage_id: '', section_id: event.target.value ? '' : taskForm.section_id })} className="w-full rounded-2xl border bg-white px-3 py-2.5"><option value="">Без проекта</option>{projects.map((project) => <option key={project.id} value={project.id}>{project.name}</option>)}</select></Field>
            <Field label="Этап"><select value={taskForm.stage_id} onChange={(event) => setTaskForm({ ...taskForm, stage_id: event.target.value })} disabled={!taskForm.project_id} className="w-full rounded-2xl border bg-white px-3 py-2.5 disabled:bg-slate-100"><option value="">Без этапа</option>{selectedProjectStages.map((stage) => <option key={stage.id} value={stage.id}>{stage.sort_order}. {stage.title}</option>)}</select></Field>
            {!taskForm.project_id && <Field label="Раздел для задачи без проекта" className="md:col-span-2"><select value={taskForm.section_id} onChange={(event) => setTaskForm({ ...taskForm, section_id: event.target.value, project_id: event.target.value ? '' : taskForm.project_id, stage_id: event.target.value ? '' : taskForm.stage_id })} className="w-full rounded-2xl border bg-white px-3 py-2.5"><option value="">Без раздела</option>{sections.map((section) => <option key={section.id} value={section.id}>{section.name}</option>)}</select><span className="mt-1 block text-xs text-slate-500">Используйте только для отдельной задачи без проекта. Для обычной работы сначала выберите проект — его раздел определится автоматически.</span></Field>}
            <Field label="Ответственный"><select value={taskForm.owner} onChange={(event) => setTaskForm({ ...taskForm, owner: event.target.value })} className="w-full rounded-2xl border bg-white px-3 py-2.5">{employeeNames.map((name) => <option key={name}>{name}</option>)}</select></Field>
            <Field label="Дедлайн"><input type="date" value={taskForm.deadline} onChange={(event) => setTaskForm({ ...taskForm, deadline: event.target.value })} className="w-full rounded-2xl border px-3 py-2.5" /></Field>
            <Field label="Начало"><input type="time" value={taskForm.start_time} onChange={(event) => setTaskForm({ ...taskForm, start_time: event.target.value })} className="w-full rounded-2xl border px-3 py-2.5" /></Field>
            <Field label="Конец"><input type="time" value={taskForm.end_time} onChange={(event) => setTaskForm({ ...taskForm, end_time: event.target.value, hours: hoursBetween(taskForm.start_time, event.target.value) || taskForm.hours })} className="w-full rounded-2xl border px-3 py-2.5" /></Field>
            <Field label="Статус"><select value={taskForm.status} onChange={(event) => setTaskForm({ ...taskForm, status: event.target.value })} className="w-full rounded-2xl border bg-white px-3 py-2.5">{TASK_STATUSES.map((status) => <option key={status}>{status}</option>)}</select></Field>
            <Field label="Приоритет"><select value={taskForm.priority} onChange={(event) => setTaskForm({ ...taskForm, priority: event.target.value })} className="w-full rounded-2xl border bg-white px-3 py-2.5">{PRIORITIES.map((priority) => <option key={priority}>{priority}</option>)}</select></Field>
            <Field label="Загрузка, часов"><input type="number" min="0" step="0.5" value={taskForm.hours} onChange={(event) => setTaskForm({ ...taskForm, hours: event.target.value })} className="w-full rounded-2xl border px-3 py-2.5" /></Field>
            <Field label="Измеримый результат" className="md:col-span-2"><textarea value={taskForm.result} onChange={(event) => setTaskForm({ ...taskForm, result: event.target.value })} rows="2" placeholder="Что будет считаться готовым результатом" className="w-full rounded-2xl border px-3 py-2.5" /></Field>
            <Field label="Комментарий" className="md:col-span-2"><textarea value={taskForm.comment} onChange={(event) => setTaskForm({ ...taskForm, comment: event.target.value })} rows="3" placeholder="Текущий ход работы, договорённость, результат или блокер" className="w-full rounded-2xl border px-3 py-2.5" /></Field>
            <Field label="Ссылка на материалы" className="md:col-span-2">
              <div className="relative"><Link2 className="absolute left-3 top-3 h-4 w-4 text-slate-400" /><input type="url" value={taskForm.resource_url} onChange={(event) => setTaskForm({ ...taskForm, resource_url: event.target.value })} placeholder="https://docs.google.com/spreadsheets/..." className="w-full rounded-2xl border px-3 py-2.5 pl-10" /></div>
              <span className="mt-1 block text-xs text-slate-500">Можно вставить Google-таблицу, документ, Miro, папку Drive или любую рабочую ссылку. После сохранения она будет открываться из карточки задачи.</span>
              {normalizeResourceUrl(taskForm.resource_url) && <div className="mt-2"><TaskResourceLink url={normalizeResourceUrl(taskForm.resource_url)} compact /></div>}
            </Field>

            {deadlineWasChanged && (
              <div className="md:col-span-2 rounded-2xl border border-rose-200 bg-gradient-to-r from-rose-50 to-orange-50 p-4">
                <div className="flex items-start gap-3"><span className="rounded-xl bg-rose-100 p-2 text-rose-600"><CalendarClock className="h-5 w-5" /></span><div><b>Изменён дедлайн: {formatDate(editingTask.deadline)} → {formatDate(taskForm.deadline)}</b><p className="mt-1 text-sm text-slate-600">После сохранения перенос появится в отдельном журнале.</p></div></div>
                <div className="mt-4 grid gap-4 md:grid-cols-2"><Field label="Кто перенёс"><select value={deadlineChange.changed_by} onChange={(event) => setDeadlineChange({ ...deadlineChange, changed_by: event.target.value })} className="w-full rounded-2xl border bg-white px-3 py-2.5">{employeeNames.map((name) => <option key={name}>{name}</option>)}</select></Field><Field label="Причина переноса"><input value={deadlineChange.reason} onChange={(event) => setDeadlineChange({ ...deadlineChange, reason: event.target.value })} placeholder="Например: ждём данные от клиента" className="w-full rounded-2xl border px-3 py-2.5" /></Field></div>
              </div>
            )}
          </div>
          <ModalActions onCancel={() => setIsTaskModalOpen(false)} onSave={saveTask} saveLabel={editingTaskId ? 'Сохранить задачу' : 'Создать задачу'} />
        </Modal>
      )}

      {isEmployeeModalOpen && (
        <Modal title="Добавить сотрудника" subtitle="Новый сотрудник сразу появится во всех списках ответственных." onClose={() => setIsEmployeeModalOpen(false)} maxWidth="max-w-lg">
          <div className="space-y-4">
            <Field label="Имя сотрудника"><div className="relative"><UserRound className="absolute left-3 top-3 h-4 w-4 text-slate-400" /><input value={employeeForm.name} onChange={(event) => setEmployeeForm({ ...employeeForm, name: event.target.value })} placeholder="Например: Мария" className="w-full rounded-2xl border px-3 py-2.5 pl-10" /></div></Field>
            <Field label="Должность или роль"><div className="relative"><Briefcase className="absolute left-3 top-3 h-4 w-4 text-slate-400" /><input value={employeeForm.role} onChange={(event) => setEmployeeForm({ ...employeeForm, role: event.target.value })} placeholder="Например: Менеджер проекта" className="w-full rounded-2xl border px-3 py-2.5 pl-10" /></div></Field>
            <Field label="Цвет"><div className="flex flex-wrap gap-2">{EMPLOYEE_COLORS.map((color) => <button key={color} type="button" onClick={() => setEmployeeForm({ ...employeeForm, color })} className={`h-9 w-9 rounded-full ${employeeForm.color === color ? 'ring-2 ring-slate-900 ring-offset-2' : ''}`} style={{ backgroundColor: color }} />)}</div></Field>
            <div className="rounded-2xl bg-slate-50 p-4"><div className="flex items-center gap-3"><span className="flex h-11 w-11 items-center justify-center rounded-2xl text-sm font-bold text-white" style={{ backgroundColor: employeeForm.color }}>{employeeInitials(employeeForm.name || 'Новый')}</span><div><b>{employeeForm.name || 'Новый сотрудник'}</b><p className="text-sm text-slate-500">{employeeForm.role || 'Сотрудник'}</p></div></div></div>
          </div>
          <ModalActions onCancel={() => setIsEmployeeModalOpen(false)} onSave={addEmployee} saveLabel="Добавить сотрудника" />
        </Modal>
      )}
    </div>
  );
}

function Modal({ title, subtitle, onClose, children, maxWidth = 'max-w-2xl' }) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center overflow-y-auto bg-slate-950/65 p-4 backdrop-blur-sm">
      <motion.div initial={{ opacity: 0, scale: 0.97 }} animate={{ opacity: 1, scale: 1 }} className={`my-auto w-full ${maxWidth} rounded-[2rem] bg-white p-5 shadow-2xl md:p-6`}>
        <div className="mb-5 flex items-start justify-between gap-4"><div><h2 className="text-2xl font-bold">{title}</h2><p className="mt-1 text-sm text-slate-500">{subtitle}</p></div><button type="button" onClick={onClose} className="rounded-full p-2 hover:bg-slate-100"><X className="h-5 w-5" /></button></div>
        {children}
      </motion.div>
    </div>
  );
}

function Field({ label, children, className = '' }) {
  return <label className={className}><span className="mb-1 block text-sm font-medium text-slate-700">{label}</span>{children}</label>;
}

function ModalActions({ onCancel, onSave, saveLabel }) {
  return <div className="mt-6 flex justify-end gap-2"><button type="button" onClick={onCancel} className="rounded-2xl border px-5 py-3 text-sm hover:bg-slate-50">Отмена</button><button type="button" onClick={onSave} className="rounded-2xl bg-violet-600 px-5 py-3 text-sm font-medium text-white hover:bg-violet-500">{saveLabel}</button></div>;
}
