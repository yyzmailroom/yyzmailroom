// ============================================================
// YYZ OFFICES — SUPABASE ADAPTER
// Drop-in replacement for the GAS api() and apiPost() functions.
// Returns the EXACT same response shapes so zero UI code changes needed.
//
// USAGE: Replace the <script> section in client.html and staff.html:
//   1. Add <script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"></script>
//   2. Replace the old API_URL, api(), and apiPost() with this file
//   3. Everything else stays exactly the same
// ============================================================

const SUPABASE_URL  = 'https://catpufkbjmcjmtuisdok.supabase.co';
const SUPABASE_KEY  = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNhdHB1Zmtiam1jam10dWlzZG9rIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjIyNDgyNSwiZXhwIjoyMDg3ODAwODI1fQ.f2lEF3S063-qlwYwXW5aU5GbIPSNwzWtXw4ZtLk7cwg';

const sb = window.supabase.createClient(SUPABASE_URL, SUPABASE_KEY);

// ── ID Generator (matches existing format) ──────────────
function _genId(prefix) {
  const c = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let id = prefix;
  for (let i = 0; i < 12; i++) id += c[Math.floor(Math.random() * c.length)];
  return id;
}

// ── camelCase ↔ snake_case helpers ──────────────────────
function toSnake(s) { return s.replace(/([A-Z])/g, '_$1').toLowerCase(); }
function toCamel(s) { return s.replace(/_([a-z])/g, (_, c) => c.toUpperCase()); }
function rowToCamel(row) {
  if (!row) return row;
  const out = {};
  for (const [k, v] of Object.entries(row)) out[toCamel(k)] = v;
  return out;
}
function rowsToCamel(rows) { return (rows || []).map(rowToCamel); }

// ============================================================
// REPLACEMENT: api(path) — handles GET-style calls
// Parses the query string and routes to the right handler
// ============================================================
async function api(path) {
  const params = new URLSearchParams(path);
  const uuid   = params.get('uuid');
  const action = params.get('action');

  if (!action) {
    // Boot call — resolveAccess
    return _resolveAccess(uuid);
  }

  switch (action) {
    case 'getTasks':            return _getTasks(uuid);
    case 'searchRecipients':    return _searchRecipients(uuid, params.get('q') || '');
    case 'getMailLogStaff':     return _getMailLogStaff(uuid);
    case 'getAgentsForPlanCard':return _getAgentsForPlanCard(uuid, params.get('planCardId'));
    case 'getExceptions':       return _getExceptions(uuid);
    case 'getPlanCardsStaff':   return _getPlanCardsStaff(uuid);
    default:
      return { status: 'error', message: 'Unknown action: ' + action };
  }
}

// ============================================================
// REPLACEMENT: apiPost(body) — handles write calls + complex reads
// ============================================================
async function apiPost(body) {
  const action = body.action;
  const uuid   = body.uuid;

  switch (action) {
    // ── Client reads ──
    case 'getMailLog':       return _getClientMailLog(uuid, body.subscriptionId);
    case 'getPlanCard':      return _getPlanCard(uuid, body.subscriptionId);
    case 'getAgents':        return _getAgents(uuid, body.subscriptionId);
    case 'getRecipients':    return _getRecipients(uuid, body.subscriptionId);
    case 'getPlanTemplate':  return _getPlanTemplate(body.productId);

    // ── Client writes ──
    case 'submitOnboarding': return _submitOnboarding(body);
    case 'addRecipient':     return _addRecipient(body);
    case 'updateFriendlyName': return _updateFriendlyName(body);
    case 'addAgent':         return _addAgent(body);
    case 'removeAgent':      return _removeAgent(body);

    // ── Staff writes ──
    case 'logMail':               return _logMail(body);
    case 'resolveTask':           return _resolveTask(body);
    case 'snoozeTask':            return _snoozeTask(body);
    case 'releaseMail':           return _releaseMail(body);
    case 'bulkForwardMail':       return _bulkForwardMail(body);
    case 'assignMailRecipient':   return _assignMailRecipient(body);
    case 'editMailItem':          return _editMailItem(body);
    case 'deleteMailItem':        return _deleteMailItem(body);
    case 'updateMailStatus':      return _updateMailStatus(body);
    case 'addTempRecipient':      return _addTempRecipient(body);
    case 'updateRecipient':       return _updateRecipient(body);
    case 'updateRecipientStatus': return _updateRecipientStatus(body);
    case 'notifyNonPayment':      return { status: 'ok' }; // TODO: email notification

    default:
      return { status: 'error', message: 'Unknown action: ' + action };
  }
}

// ============================================================
// RESOLVE ACCESS (boot call)
// Must return the exact shape the frontend expects
// ============================================================
async function _resolveAccess(uuid) {
  if (!uuid) return { status: 'error', message: 'No UUID provided' };

  // Check staff first
  const { data: staff } = await sb.from('staff').select('*')
    .eq('staff_id', uuid).eq('active', true).maybeSingle();
  if (staff) {
    return {
      status:     'staff',
      role:       staff.role,
      name:       staff.name,
      email:      staff.email,
      staffId:    staff.staff_id,
      locationId: staff.default_location_id,
      pin:        staff.pin
    };
  }

  // Check client
  const { data: client } = await sb.from('clients').select('*')
    .eq('id', uuid).maybeSingle();
  if (!client || client.status !== 'active') {
    return { status: 'blocked', message: 'Account not found or inactive.' };
  }

  // Get all subscriptions
  const { data: subs } = await sb.from('subscriptions').select('*')
    .eq('client_id', uuid).order('created_at', { ascending: false });

  // Get all plan cards for this client
  const { data: planCards } = await sb.from('plan_cards').select('*')
    .eq('client_id', uuid);

  const planCardMap = {};
  (planCards || []).forEach(pc => { planCardMap[pc.subscription_id] = pc; });

  // Build subscription objects matching GAS response shape
  const subscriptions = (subs || []).map(s => {
    const pc = planCardMap[s.id];
    const accessStatus = s.access_status || 'ACTIVE';
    const canAccess = ['ACTIVE', 'CANCELED_WITH_ACCESS'].includes(accessStatus);
    
    // Setup is required if: no plan card, OR plan card exists but no recipients added yet
    const needsPlanCard = !pc;
    const needsRecipients = pc && pc.recipients_added === 0;
    const setupRequired = canAccess && (needsPlanCard || needsRecipients);
    
    // Determine which setup step they're on
    let setupStep = null;
    if (setupRequired) {
      setupStep = needsPlanCard ? 'plan_setup' : 'add_recipients';
    }

    // Determine banner
    let banner = null;
    if (accessStatus === 'PAYMENT_REQUIRED') {
      banner = { type: 'payment_required', title: 'Payment Required',
        message: 'Your payment could not be processed. Please update your payment method.',
        actionLabel: 'Update Payment', actionUrl: 'https://dashboard.assembly.com' };
    } else if (accessStatus === 'CANCELED_WITH_ACCESS') {
      const until = s.access_until_date ? new Date(s.access_until_date).toLocaleDateString('en-CA', { month:'short', day:'numeric', year:'numeric' }) : '';
      banner = { type: 'canceled_with_access', title: 'Subscription Ending',
        message: `Your subscription is ending on ${until}. Renew to keep access.`,
        actionLabel: 'Renew', actionUrl: 'https://dashboard.assembly.com' };
    } else if (accessStatus === 'CANCELED') {
      banner = { type: 'canceled', title: 'Subscription Ended',
        message: 'Your subscription has ended. Reactivate to regain access.',
        actionLabel: 'Reactivate', actionUrl: 'https://dashboard.assembly.com' };
    } else if (setupRequired) {
      banner = { type: 'setup_required', title: 'Setup Required',
        message: 'Complete your mailbox setup to start receiving mail.',
        actionLabel: 'Set Up Now', setupStep: 'plan_setup' };
    }

    return {
      subscriptionId: s.id,
      planName:       s.plan_name,
      planAmount:     s.plan_amount,
      interval:       s.interval,
      productId:      s.product_id,
      accessStatus,
      canAccess,
      setupRequired,
      setupStep,
      planCardId:     pc ? pc.plan_card_id : null,
      friendlyName:   pc ? pc.friendly_name : null,
      banner
    };
  });

  return {
    status:        'client',
    clientId:      client.id,
    name:          [client.given_name, client.family_name].filter(Boolean).join(' '),
    givenName:     client.given_name,
    familyName:    client.family_name,
    email:         client.email,
    fallbackColor: client.fallback_color,
    subscriptions
  };
}

// ============================================================
// CLIENT READS
// ============================================================

async function _getClientMailLog(uuid, subscriptionId) {
  const { data, error } = await sb.from('mail_log')
    .select('*')
    .eq('client_id', uuid)
    .eq('subscription_id', subscriptionId)
    .neq('status', 'deleted')
    .order('logged_at', { ascending: false });
  if (error) return { status: 'error', message: error.message };

  // Strip staff-only fields
  const mailLog = (data || []).map(m => {
    const r = rowToCamel(m);
    delete r.noteInternal;
    delete r.physicalLocation;
    return r;
  });
  return { status: 'ok', mailLog };
}

async function _getPlanCard(uuid, subscriptionId) {
  const { data, error } = await sb.from('plan_cards').select('*')
    .eq('client_id', uuid).eq('subscription_id', subscriptionId).maybeSingle();
  if (error) return { status: 'error', message: error.message };
  return { status: 'ok', planCard: data ? rowToCamel(data) : null };
}

async function _getAgents(uuid, subscriptionId) {
  const { data } = await sb.from('pickup_agents').select('*')
    .eq('client_id', uuid).in('status', ['active', 'inactive'])
    .order('added_at');
  return { status: 'ok', agents: rowsToCamel(data) };
}

async function _getRecipients(uuid, subscriptionId) {
  const { data } = await sb.from('recipients').select('*')
    .eq('client_id', uuid).eq('subscription_id', subscriptionId)
    .order('created_at');
  return { status: 'ok', recipients: rowsToCamel(data) };
}

async function _getPlanTemplate(productId) {
  if (!productId) return { status: 'error', message: 'No productId' };
  const { data: tmpl } = await sb.from('plans').select('*')
    .eq('product_id', productId).maybeSingle();
  const { data: allLocs } = await sb.from('locations').select('*').eq('active', true);

  // Filter locations based on plan's locations field (comma-separated IDs)
  let locs = allLocs || [];
  if (tmpl && tmpl.locations) {
    const allowedIds = tmpl.locations.split(',').map(s => s.trim());
    locs = locs.filter(l => allowedIds.includes(l.location_id));
  }

  return {
    status: 'ok',
    template: tmpl ? rowToCamel(tmpl) : null,
    locations: rowsToCamel(locs)
  };
}

// ============================================================
// CLIENT WRITES
// ============================================================

async function _submitOnboarding(body) {
  const productId = body.productId;
  const { data: tmpl } = await sb.from('plans').select('*')
    .eq('product_id', productId).maybeSingle();
  if (!tmpl) return { status: 'error', message: 'Plan template not found' };

  const planCardId = _genId('PC');
  const now = new Date().toISOString();
  const today = now.split('T')[0];
  const periodStart = today;
  // Calculate renewal date (next billing date)
  const startParts = today.split('-').map(Number);
  let renewYear = startParts[0], renewMonth = startParts[1], renewDay = startParts[2];
  if (tmpl.billing_cycle === 'yearly') {
    renewYear += 1;
  } else {
    renewMonth += 1;
    if (renewMonth > 12) { renewMonth = 1; renewYear += 1; }
  }
  // Cap day to last day of renewal month
  const lastDay = new Date(renewYear, renewMonth, 0).getDate();
  renewDay = Math.min(renewDay, lastDay);
  // period_end = last day of access (renewal date minus 1)
  const renewDate = new Date(renewYear, renewMonth - 1, renewDay);
  renewDate.setDate(renewDate.getDate() - 1);
  const periodEnd = `${renewDate.getFullYear()}-${String(renewDate.getMonth() + 1).padStart(2,'0')}-${String(renewDate.getDate()).padStart(2,'0')}`;

  const { error } = await sb.from('plan_cards').insert({
    plan_card_id:       planCardId,
    client_id:          body.uuid,
    subscription_id:    body.subscriptionId,
    location_id:        body.locationId || 'LOC001',
    plan_name:          tmpl.plan_name,
    billing_cycle:      tmpl.billing_cycle,
    status:             'active',
    mail_limit:         tmpl.mail_limit,
    parcel_limit:       tmpl.parcels_included || 0,
    max_recipients:     tmpl.max_recipients,
    recipients_added:   0,
    mail_storage_days:  tmpl.mail_storage_days,
    parcel_storage_days:tmpl.parcel_storage_days,
    mail_overage_fee:   tmpl.mail_overage_fee,
    parcel_overage_fee: tmpl.parcel_overage_fee,
    current_period_start: today,
    current_period_end:   periodEnd,
    auto_forward_day:     tmpl.auto_forward_day,
    forwarding_address:   body.forwardingAddress || null,
    forwarding_city:      body.forwardingCity || null,
    forwarding_province:  body.forwardingProvince || null,
    forwarding_postal_code: body.forwardingPostalCode || null,
    forwarding_country:     body.forwardingCountry || null,
    forwarding_instructions: body.forwardingInstructions || null,
    client_timezone:     body.clientTimezone || 'America/Toronto',
    business_description: body.businessDescription || null,
    referral_source:     body.referralSource || null,
    activated_at:        null,
    activated_by:        null,
    friendly_name:       body.friendlyName || null,
    auto_feature:        tmpl.auto_feature,
    product_id:          productId,
    plan_memo:           tmpl.plan_memo,
    customer_type:       body.customerType || 'canadian'
  });
  if (error) return { status: 'error', message: error.message };

  return { status: 'ok', planCardId };
}

async function _addRecipient(body) {
  const planCardId = body.planCardId;
  const { data: pc } = await sb.from('plan_cards')
    .select('max_recipients, recipients_added, subscription_id, location_id, client_id')
    .eq('plan_card_id', planCardId).single();
  if (!pc) return { status: 'error', message: 'Plan card not found' };

  const isTemp = body.notes && body.notes.startsWith('TEMP:');

  if (!isTemp) {
    // Count actual active non-temp recipients (source of truth)
    const { data: activeRecs } = await sb.from('recipients').select('recipient_id')
      .eq('plan_card_id', planCardId).eq('status', 'active')
      .not('notes', 'like', 'TEMP:%');
    const activeCount = activeRecs ? activeRecs.length : 0;
    if (activeCount >= pc.max_recipients) {
      return { status: 'error', message: `Maximum active recipients reached (${pc.max_recipients}). Deactivate one first or add as a temporary recipient.` };
    }
  }

  const recipientId = _genId('RCP');
  const now = new Date().toISOString();
  // body.uuid may be a staff ID (e.g. "STF001") which isn't a valid UUID
  const actorUuid = /^[0-9a-f]{8}-/i.test(body.uuid) ? body.uuid : null;
  const { error } = await sb.from('recipients').insert({
    recipient_id:   recipientId,
    plan_card_id:   planCardId,
    client_id:      pc.client_id,
    subscription_id: pc.subscription_id,
    location_id:    pc.location_id,
    name:           body.name,
    type:           body.type || 'individual',
    status:         'active',
    activated_at:   now,
    activated_by:   actorUuid,
    has_mail_logged: false,
    language:       body.language || 'en',
    created_at:     now,
    created_by:     actorUuid,
    notes:          body.notes || null
  });
  if (error) return { status: 'error', message: error.message };

  if (!isTemp) {
    // Sync recipients_added counter with actual active count
    const { data: nowActive } = await sb.from('recipients').select('recipient_id')
      .eq('plan_card_id', planCardId).eq('status', 'active')
      .not('notes', 'like', 'TEMP:%');
    const newCount = nowActive ? nowActive.length : (pc.recipients_added + 1);
    const updates = { recipients_added: newCount };
    // Activate plan card on first recipient added (completes setup)
    if (pc.recipients_added === 0) {
      updates.activated_at = now;
      updates.activated_by = actorUuid;
    }
    await sb.from('plan_cards').update(updates).eq('plan_card_id', planCardId);

    // Resolve pending_setup task when first recipient added
    if (pc.recipients_added === 0) {
      await sb.from('tasks')
        .update({ status: 'resolved', resolved_at: now, resolution_note: 'Recipients added, setup complete' })
        .eq('client_id', pc.client_id).eq('type', 'pending_setup').eq('status', 'open');
    }
  }
  return { status: 'ok', recipientId };
}

async function _updateFriendlyName(body) {
  const { error } = await sb.from('plan_cards')
    .update({ friendly_name: body.friendlyName })
    .eq('plan_card_id', body.planCardId);
  if (error) return { status: 'error', message: error.message };
  return { status: 'ok' };
}

async function _addAgent(body) {
  // Check max 5 agents per client
  const { data: existing } = await sb.from('pickup_agents').select('agent_id')
    .eq('client_id', body.uuid).eq('status', 'active');
  if (existing && existing.length >= 5) {
    return { status: 'error', message: 'Maximum 5 pickup agents allowed.' };
  }

  const agentId = _genId('AGT');
  const now = new Date().toISOString();
  const { error } = await sb.from('pickup_agents').insert({
    agent_id:     agentId,
    plan_card_id: body.planCardId || null,
    client_id:    body.uuid,
    location_id:  body.locationId || null,
    name:         body.name,
    id_type:      body.idType || null,
    id_last4:     body.idLast4 || null,
    phone:        body.phone || null,
    status:       'active',
    added_at:     now,
    added_by:     body.uuid,
    notes:        body.notes || null
  });
  if (error) return { status: 'error', message: error.message };
  return { status: 'ok', agentId };
}

async function _removeAgent(body) {
  // This now toggles between active/inactive
  const { data: agent } = await sb.from('pickup_agents').select('status, client_id')
    .eq('agent_id', body.agentId).maybeSingle();
  if (!agent) return { status: 'error', message: 'Agent not found' };

  const now = new Date().toISOString();
  const newStatus = agent.status === 'active' ? 'inactive' : 'active';

  // If reactivating, check max 5 active
  if (newStatus === 'active') {
    const { data: activeAgents } = await sb.from('pickup_agents').select('agent_id')
      .eq('client_id', agent.client_id).eq('status', 'active');
    if (activeAgents && activeAgents.length >= 5) {
      return { status: 'error', message: 'Maximum 5 active agents allowed. Deactivate one first.' };
    }
  }

  const updates = { status: newStatus };
  if (newStatus === 'inactive') {
    updates.deactivated_at = now;
    updates.deactivated_by = body.uuid;
  } else {
    updates.deactivated_at = null;
    updates.deactivated_by = null;
  }

  const { error } = await sb.from('pickup_agents').update(updates).eq('agent_id', body.agentId);
  if (error) return { status: 'error', message: error.message };
  return { status: 'ok', newStatus };
}

// ============================================================
// STAFF READS
// ============================================================

async function _getTasks(uuid) {
  const { data: staff } = await sb.from('staff').select('default_location_id')
    .eq('staff_id', uuid).maybeSingle();
  const locId = staff?.default_location_id;

  const { data } = await sb.from('tasks').select('*')
    .in('status', ['open', 'snoozed'])
    .order('due_date');

  // Filter: tasks for this location or global (null location)
  const filtered = (data || []).filter(t => !t.location_id || t.location_id === locId);
  return { status: 'ok', tasks: rowsToCamel(filtered) };
}

async function _searchRecipients(uuid, q) {
  const { data: staff } = await sb.from('staff').select('default_location_id')
    .eq('staff_id', uuid).maybeSingle();
  const locId = staff?.default_location_id;

  const { data } = await sb.from('recipients')
    .select('*, plan_cards!inner(plan_card_id, client_id, subscription_id, plan_name, auto_feature, status)')
    .eq('location_id', locId)
    .eq('status', 'active');

  // Also fetch subscription access_status for each
  const enriched = [];
  for (const r of (data || [])) {
    const pc = r.plan_cards;
    let accessStatus = 'ACTIVE';
    if (pc?.subscription_id) {
      const { data: sub } = await sb.from('subscriptions').select('access_status')
        .eq('id', pc.subscription_id).maybeSingle();
      if (sub) accessStatus = sub.access_status;
    }
    enriched.push({
      recipientId:    r.recipient_id,
      name:           r.name,
      companyName:    '',
      type:           r.type,
      planCardId:     pc?.plan_card_id,
      clientId:       pc?.client_id,
      subscriptionId: pc?.subscription_id,
      planName:       pc?.plan_name,
      autoFeature:    pc?.auto_feature,
      accessStatus,
      recipientStatus: r.status,
      planStatus:      pc?.status
    });
  }
  return enriched; // Note: searchRecipients GAS returns array directly, not {status:'ok',...}
}

async function _getMailLogStaff(uuid) {
  const { data: staff } = await sb.from('staff').select('default_location_id')
    .eq('staff_id', uuid).maybeSingle();
  const locId = staff?.default_location_id;

  const { data } = await sb.from('mail_log').select('*')
    .eq('location_id', locId).neq('status', 'deleted')
    .order('logged_at', { ascending: false }).limit(200);

  // Enrich with access_status from subscription
  const enriched = [];
  const subCache = {};
  for (const m of (data || [])) {
    let accessStatus = m.subscription_status;
    if (!accessStatus && m.subscription_id) {
      if (!subCache[m.subscription_id]) {
        const { data: s } = await sb.from('subscriptions').select('access_status').eq('id', m.subscription_id).maybeSingle();
        subCache[m.subscription_id] = s?.access_status || null;
      }
      accessStatus = subCache[m.subscription_id];
    }
    const row = rowToCamel(m);
    row.accessStatus = accessStatus;
    enriched.push(row);
  }
  return { status: 'ok', mailLog: enriched };
}

async function _getAgentsForPlanCard(uuid, planCardId) {
  const { data } = await sb.from('pickup_agents').select('*')
    .eq('plan_card_id', planCardId).eq('status', 'active');
  return { status: 'ok', agents: rowsToCamel(data) };
}

async function _getExceptions(uuid) {
  const { data: staff } = await sb.from('staff').select('default_location_id')
    .eq('staff_id', uuid).maybeSingle();
  const locId = staff?.default_location_id;

  const { data } = await sb.from('mail_log').select('*')
    .eq('location_id', locId).eq('special_case', true)
    .in('status', ['received'])
    .order('logged_at', { ascending: false }).limit(200);

  return { status: 'ok', exceptions: rowsToCamel(data) };
}

async function _getPlanCardsStaff(uuid) {
  const { data: staff } = await sb.from('staff').select('default_location_id')
    .eq('staff_id', uuid).maybeSingle();
  const locId = staff?.default_location_id;

  const { data: cards } = await sb.from('plan_cards').select('*')
    .eq('location_id', locId).eq('status', 'active');

  // Enrich each card with client info, subscription status, recipients
  const enriched = [];
  for (const pc of (cards || [])) {
    const { data: client } = await sb.from('clients').select('given_name, family_name, email, fallback_color')
      .eq('id', pc.client_id).maybeSingle();
    const { data: sub } = await sb.from('subscriptions').select('access_status, plan_name, plan_amount_formatted')
      .eq('id', pc.subscription_id).maybeSingle();
    const { data: recs } = await sb.from('recipients').select('*')
      .eq('plan_card_id', pc.plan_card_id).order('created_at');

    const card = rowToCamel(pc);
    card.clientName = client ? [client.given_name, client.family_name].filter(Boolean).join(' ') : '';
    card.clientEmail = client?.email || '';
    card.clientColor = client?.fallback_color || '';
    card.accessStatus = sub?.access_status || 'ACTIVE';
    card.subscriptionPlanName = sub?.plan_name || '';
    card.planAmountFormatted = sub?.plan_amount_formatted || '';
    card.recipients = rowsToCamel(recs);
    enriched.push(card);
  }

  return { status: 'ok', planCards: enriched };
}

// ============================================================
// STAFF WRITES
// ============================================================

async function _logMail(body) {
  const mailId = _genId('ML');
  const now = new Date().toISOString();
  const today = now.split('T')[0];

  let storageDays = 30;
  if (body.planCardId) {
    const { data: pc } = await sb.from('plan_cards')
      .select('mail_storage_days, parcel_storage_days')
      .eq('plan_card_id', body.planCardId).maybeSingle();
    if (pc) storageDays = body.type === 'parcel' ? pc.parcel_storage_days : pc.mail_storage_days;
  }
  const due = new Date(today);
  due.setDate(due.getDate() + storageDays);

  // Get subscription access_status
  let subStatus = null;
  if (body.subscriptionId) {
    const { data: s } = await sb.from('subscriptions').select('access_status')
      .eq('id', body.subscriptionId).maybeSingle();
    subStatus = s?.access_status || null;
  }

  const { error } = await sb.from('mail_log').insert({
    mail_id:              mailId,
    logged_at:            now,
    logged_by:            body.uuid,
    location_id:          body.locationId,
    recipient_id:         body.recipientId || null,
    recipient_name:       body.recipientName || null,
    plan_card_id:         body.planCardId || null,
    client_id:            body.clientId || null,
    subscription_id:      body.subscriptionId || null,
    subscription_status:  subStatus,
    special_case:         body.specialCase === true || body.specialCase === 'true',
    special_case_reason:  body.specialCaseReason || null,
    type:                 body.type || 'letter',
    confidential:         body.confidential === true || body.confidential === 'true',
    sender_name:          body.senderName || null,
    physical_location:    body.physicalLocation || null,
    scan_image_url:       body.scanImageUrl || null,
    note_to_client:       body.noteToClient || null,
    note_internal:        body.noteInternal || null,
    oversized_pickup:     body.oversizedPickup === true || body.oversizedPickup === 'true',
    piece_count:          parseInt(body.pieceCount) || 1,
    estimated_weight:     body.estimatedWeight || null,
    return_address:       body.returnAddress || null,
    status:               'received',
    storage_start_date:   today,
    storage_due_date:     due.toISOString().split('T')[0]
  });
  if (error) return { status: 'error', message: error.message };

  // Increment usage counter
  if (body.planCardId && body.specialCase !== true && body.specialCase !== 'true') {
    const field = body.type === 'parcel' ? 'parcels_used' : 'mails_used';
    const { data: pc } = await sb.from('plan_cards').select(field + ', mail_limit, parcel_limit, mail_overage_fee, parcel_overage_fee')
      .eq('plan_card_id', body.planCardId).single();
    if (pc) {
      await sb.from('plan_cards').update({ [field]: pc[field] + 1 }).eq('plan_card_id', body.planCardId);
    }
    // Mark recipient has_mail_logged
    if (body.recipientId) {
      await sb.from('recipients').update({ has_mail_logged: true }).eq('recipient_id', body.recipientId);
    }
  }

  return { status: 'ok', mailId };
}

async function _resolveTask(body) {
  const { error } = await sb.from('tasks').update({
    status: 'resolved',
    resolved_at: new Date().toISOString(),
    resolved_by: body.uuid,
    resolution_note: body.resolutionNote || null
  }).eq('task_id', body.taskId);
  if (error) return { status: 'error', message: error.message };
  return { status: 'ok' };
}

async function _snoozeTask(body) {
  const { error } = await sb.from('tasks').update({
    status: 'snoozed',
    snoozed_until: body.snoozeUntil
  }).eq('task_id', body.taskId);
  if (error) return { status: 'error', message: error.message };
  return { status: 'ok' };
}

async function _releaseMail(body) {
  // Look up agent name
  let releasedTo = body.agentId;
  if (body.agentId) {
    const { data: agent } = await sb.from('pickup_agents').select('name')
      .eq('agent_id', body.agentId).maybeSingle();
    if (agent) releasedTo = agent.name;
  }

  const { error } = await sb.from('mail_log').update({
    status: 'released',
    released_at: new Date().toISOString(),
    released_by: body.uuid,
    released_to: releasedTo,
    release_notes: body.releaseNotes || null
  }).eq('mail_id', body.mailId);
  if (error) return { status: 'error', message: error.message };
  return { status: 'ok' };
}

async function _bulkForwardMail(body) {
  const mailIds = JSON.parse(body.mailIds || '[]');
  const results = [];
  for (const mailId of mailIds) {
    const { error } = await sb.from('mail_log').update({
      status: 'forwarded',
      forwarded_at: new Date().toISOString(),
      tracking_link: body.trackingLink || null,
      forwarding_cost: body.forwardingCost ? parseFloat(body.forwardingCost) : null,
      note_internal: body.forwardNotes || null
    }).eq('mail_id', mailId);
    results.push({ mailId, status: error ? 'error' : 'ok', message: error?.message });
  }
  return { status: 'ok', results };
}

async function _assignMailRecipient(body) {
  const today = new Date().toISOString().split('T')[0];
  let storageDays = 30;
  if (body.planCardId) {
    const { data: pc } = await sb.from('plan_cards').select('mail_storage_days')
      .eq('plan_card_id', body.planCardId).maybeSingle();
    if (pc) storageDays = pc.mail_storage_days;
  }
  const due = new Date(today);
  due.setDate(due.getDate() + storageDays);

  // Check if plan uses scan feature
  let needsScan = false;
  if (body.planCardId) {
    const { data: pc } = await sb.from('plan_cards').select('auto_feature')
      .eq('plan_card_id', body.planCardId).maybeSingle();
    if (pc?.auto_feature === 'scan') needsScan = true;
  }

  const { error } = await sb.from('mail_log').update({
    recipient_id: body.recipientId,
    recipient_name: body.recipientName,
    plan_card_id: body.planCardId,
    client_id: body.clientId,
    subscription_id: body.subscriptionId,
    special_case: false,
    special_case_reason: null,
    storage_start_date: today,
    storage_due_date: due.toISOString().split('T')[0]
  }).eq('mail_id', body.mailId);
  if (error) return { status: 'error', message: error.message };
  return { status: 'ok', needsScan };
}

async function _editMailItem(body) {
  const updates = {};
  const fieldMap = {
    senderName: 'sender_name', type: 'type', confidential: 'confidential',
    physicalLocation: 'physical_location', scanImageUrl: 'scan_image_url',
    noteToClient: 'note_to_client', noteInternal: 'note_internal',
    oversizedPickup: 'oversized_pickup', pieceCount: 'piece_count',
    recipientId: 'recipient_id', recipientName: 'recipient_name',
    planCardId: 'plan_card_id', clientId: 'client_id', subscriptionId: 'subscription_id',
    specialCase: 'special_case', specialCaseReason: 'special_case_reason'
  };
  for (const [camel, snake] of Object.entries(fieldMap)) {
    if (body[camel] !== undefined) {
      let val = body[camel];
      if (val === 'TRUE' || val === 'true') val = true;
      if (val === 'FALSE' || val === 'false') val = false;
      updates[snake] = val;
    }
  }
  if (Object.keys(updates).length === 0) return { status: 'ok' };

  const { error } = await sb.from('mail_log').update(updates).eq('mail_id', body.mailId);
  if (error) return { status: 'error', message: error.message };
  return { status: 'ok' };
}

async function _deleteMailItem(body) {
  // Soft delete
  const { error } = await sb.from('mail_log')
    .update({ status: 'deleted' })
    .eq('mail_id', body.mailId);
  if (error) return { status: 'error', message: error.message };
  return { status: 'ok' };
}

async function _updateMailStatus(body) {
  const updates = { status: body.status };
  if (body.note) updates.note_internal = body.note;
  const { error } = await sb.from('mail_log').update(updates).eq('mail_id', body.mailId);
  if (error) return { status: 'error', message: error.message };
  return { status: 'ok' };
}

async function _addTempRecipient(body) {
  return _addRecipient({
    ...body,
    action: 'addRecipient',
    notes: 'TEMP: ' + (body.notes || '')
  });
}

async function _updateRecipient(body) {
  const updates = { name: body.name };
  if (body.type) updates.type = body.type;
  const { error } = await sb.from('recipients').update(updates)
    .eq('recipient_id', body.recipientId);
  if (error) return { status: 'error', message: error.message };
  return { status: 'ok' };
}

async function _updateRecipientStatus(body) {
  // Get recipient details
  const { data: rec } = await sb.from('recipients').select('plan_card_id, notes, status')
    .eq('recipient_id', body.recipientId).maybeSingle();
  if (!rec) return { status: 'error', message: 'Recipient not found' };

  const isTemp = rec.notes && rec.notes.startsWith('TEMP:');
  const isReactivating = rec.status !== 'active' && body.status === 'active';

  // If reactivating a non-temp recipient, check max limit
  if (isReactivating && !isTemp && rec.plan_card_id) {
    const { data: pc } = await sb.from('plan_cards').select('max_recipients')
      .eq('plan_card_id', rec.plan_card_id).maybeSingle();
    if (pc) {
      const { data: activeRecs } = await sb.from('recipients').select('recipient_id')
        .eq('plan_card_id', rec.plan_card_id).eq('status', 'active')
        .not('notes', 'like', 'TEMP:%');
      const activeCount = activeRecs ? activeRecs.length : 0;
      if (activeCount >= pc.max_recipients) {
        return { status: 'error', message: `Cannot reactivate — maximum active recipients reached (${pc.max_recipients}). Deactivate another first, or add as a temporary recipient.` };
      }
    }
  }

  const { error } = await sb.from('recipients')
    .update({ status: body.status })
    .eq('recipient_id', body.recipientId);
  if (error) return { status: 'error', message: error.message };

  // Sync recipients_added counter on plan card for non-temp recipients
  if (!isTemp && rec.plan_card_id) {
    const { data: activeRecs } = await sb.from('recipients').select('recipient_id')
      .eq('plan_card_id', rec.plan_card_id).eq('status', 'active')
      .not('notes', 'like', 'TEMP:%');
    const count = activeRecs ? activeRecs.length : 0;
    await sb.from('plan_cards').update({ recipients_added: count })
      .eq('plan_card_id', rec.plan_card_id);
  }

  return { status: 'ok' };
}
