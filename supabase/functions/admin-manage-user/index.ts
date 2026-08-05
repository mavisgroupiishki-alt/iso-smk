import { createClient } from 'npm:@supabase/supabase-js@2';

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
};

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, 'Content-Type': 'application/json; charset=utf-8' },
  });
}

async function emailForLogin(login: string) {
  const normalized = login.trim().toLowerCase();
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(normalized));
  const hex = Array.from(new Uint8Array(digest)).map((value) => value.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 28)}@mavis.local`;
}

Deno.serve(async (request) => {
  if (request.method === 'OPTIONS') return new Response('ok', { headers: corsHeaders });
  if (request.method !== 'POST') return json({ error: 'Method not allowed' }, 405);

  try {
    const supabaseUrl = Deno.env.get('SUPABASE_URL');
    const publishableKey = Deno.env.get('SUPABASE_ANON_KEY') || Deno.env.get('SUPABASE_PUBLISHABLE_KEY');
    const serviceRoleKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');
    if (!supabaseUrl || !publishableKey || !serviceRoleKey) throw new Error('Edge Function secrets are not configured.');

    const authorization = request.headers.get('Authorization') || '';
    const userClient = createClient(supabaseUrl, publishableKey, {
      global: { headers: { Authorization: authorization } },
      auth: { persistSession: false },
    });
    const adminClient = createClient(supabaseUrl, serviceRoleKey, {
      auth: { persistSession: false, autoRefreshToken: false },
    });

    const { data: userData, error: userError } = await userClient.auth.getUser();
    if (userError || !userData.user) return json({ error: 'Требуется вход в приложение.' }, 401);

    const { data: caller } = await adminClient
      .from('app_users')
      .select('is_admin,is_active')
      .eq('auth_user_id', userData.user.id)
      .maybeSingle();
    if (!caller?.is_admin || !caller?.is_active) return json({ error: 'Управлять сотрудниками может только Аня.' }, 403);

    const body = await request.json();
    const action = String(body.action || '');

    if (action === 'create') {
      const name = String(body.name || '').trim();
      const login = String(body.login || name).trim();
      const password = String(body.password || '');
      if (!name || !login) return json({ error: 'Укажите имя и логин.' }, 400);
      if (password.length < 8) return json({ error: 'Пароль должен содержать минимум 8 символов.' }, 400);

      const { data: existingLogin } = await adminClient
        .from('app_users')
        .select('auth_user_id')
        .ilike('login', login)
        .maybeSingle();
      if (existingLogin) return json({ error: 'Такой логин уже зарегистрирован.' }, 409);

      const internalEmail = await emailForLogin(login);
      const { data: createdUser, error: createError } = await adminClient.auth.admin.createUser({
        email: internalEmail,
        password,
        email_confirm: true,
        user_metadata: { name, login },
        app_metadata: { app_role: 'employee' },
      });
      if (createError || !createdUser.user) throw createError || new Error('Не удалось создать пользователя.');

      const { data: employee, error: employeeError } = await adminClient
        .from('employees')
        .insert({
          name,
          role: String(body.role || 'Сотрудник').trim(),
          color: String(body.color || '#E7D8FF'),
          login,
          auth_user_id: createdUser.user.id,
          task_capacity: Math.max(1, Number(body.task_capacity || 10)),
          is_active: body.is_active !== false,
        })
        .select('*')
        .single();
      if (employeeError) {
        await adminClient.auth.admin.deleteUser(createdUser.user.id);
        throw employeeError;
      }

      const { error: profileError } = await adminClient.from('app_users').insert({
        auth_user_id: createdUser.user.id,
        employee_id: employee.id,
        login,
        internal_email: internalEmail,
        is_admin: false,
        is_active: body.is_active !== false,
      });
      if (profileError) {
        await adminClient.from('employees').delete().eq('id', employee.id);
        await adminClient.auth.admin.deleteUser(createdUser.user.id);
        throw profileError;
      }
      return json({ employee });
    }

    if (action === 'update') {
      const employeeId = String(body.employee_id || '');
      if (!employeeId) return json({ error: 'Не указан сотрудник.' }, 400);
      const { data: employee, error: lookupError } = await adminClient
        .from('employees')
        .select('*')
        .eq('id', employeeId)
        .single();
      if (lookupError || !employee) throw lookupError || new Error('Сотрудник не найден.');

      const { data: profile } = await adminClient
        .from('app_users')
        .select('*')
        .eq('employee_id', employeeId)
        .maybeSingle();
      if (!profile) return json({ error: 'У сотрудника ещё нет учётной записи. Создайте его заново через форму добавления.' }, 400);

      const oldName = employee.name;
      const name = String(body.name || employee.name).trim();
      const login = String(body.login || profile.login).trim();
      const password = String(body.password || '');
      const internalEmail = login.toLowerCase() === String(profile.login).toLowerCase()
        ? profile.internal_email
        : await emailForLogin(login);

      if (login.toLowerCase() !== String(profile.login).toLowerCase()) {
        const { data: existingLogin } = await adminClient
          .from('app_users')
          .select('auth_user_id')
          .ilike('login', login)
          .neq('auth_user_id', profile.auth_user_id)
          .maybeSingle();
        if (existingLogin) return json({ error: 'Такой логин уже занят.' }, 409);
      }

      const authUpdate: Record<string, unknown> = {
        email: internalEmail,
        user_metadata: { name, login },
      };
      if (password) {
        if (password.length < 8) return json({ error: 'Новый пароль должен содержать минимум 8 символов.' }, 400);
        authUpdate.password = password;
      }
      const { error: authError } = await adminClient.auth.admin.updateUserById(profile.auth_user_id, authUpdate);
      if (authError) throw authError;

      const isActive = body.is_active !== false;
      const { data: updatedEmployee, error: updateError } = await adminClient
        .from('employees')
        .update({
          name,
          role: String(body.role || employee.role || 'Сотрудник').trim(),
          color: String(body.color || employee.color || '#E7D8FF'),
          login,
          task_capacity: Math.max(1, Number(body.task_capacity || employee.task_capacity || 10)),
          is_active: isActive,
        })
        .eq('id', employeeId)
        .select('*')
        .single();
      if (updateError) throw updateError;

      const { error: profileUpdateError } = await adminClient
        .from('app_users')
        .update({ login, internal_email: internalEmail, is_active: isActive })
        .eq('employee_id', employeeId);
      if (profileUpdateError) throw profileUpdateError;

      if (oldName !== name) {
        await Promise.all([
          adminClient.from('tasks').update({ owner: name }).eq('owner', oldName),
          adminClient.from('projects').update({ owner: name }).eq('owner', oldName),
          adminClient.from('projects').update({ customer: name }).eq('customer', oldName),
          adminClient.from('project_stages').update({ owner: name }).eq('owner', oldName),
          adminClient.from('task_sections').update({ owner: name }).eq('owner', oldName),
          adminClient.from('task_reschedules').update({ changed_by: name }).eq('changed_by', oldName),
          adminClient.from('project_templates').update({ owner: name }).eq('owner', oldName),
        ]);
      }
      return json({ employee: updatedEmployee });
    }

    if (action === 'deactivate') {
      const employeeId = String(body.employee_id || '');
      const { data: profile, error } = await adminClient
        .from('app_users')
        .select('auth_user_id,is_admin')
        .eq('employee_id', employeeId)
        .single();
      if (error) throw error;
      if (profile.is_admin) return json({ error: 'Нельзя отключить единственного администратора Аню.' }, 400);
      await adminClient.from('app_users').update({ is_active: false }).eq('employee_id', employeeId);
      await adminClient.from('employees').update({ is_active: false }).eq('id', employeeId);
      return json({ ok: true });
    }

    return json({ error: 'Неизвестное действие.' }, 400);
  } catch (error) {
    return json({ error: error instanceof Error ? error.message : String(error) }, 500);
  }
});
