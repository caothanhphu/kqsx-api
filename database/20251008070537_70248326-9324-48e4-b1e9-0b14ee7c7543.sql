-- Create enums
create type game_category as enum ('regional', 'vietlott', 'keno', 'bingo', 'lotto');
create type prize_level as enum ('special','first','second','third','fourth','fifth','sixth','seventh','eighth','consolation','jackpot','other');
create type draw_status as enum ('scheduled','in_progress','completed','cancelled','void');
create type ticket_status as enum ('pending','processing','won','lost','invalid');

-- Reference Tables
create table regions (
    id serial primary key,
    code text unique not null,
    name text not null,
    created_at timestamptz default now(),
    updated_at timestamptz default now()
);

create table provinces (
    id serial primary key,
    region_id int references regions(id),
    code text unique not null,
    name text not null,
    created_at timestamptz default now(),
    updated_at timestamptz default now()
);

create table lottery_games (
    id uuid primary key default gen_random_uuid(),
    code text unique not null,
    name text not null,
    category game_category not null,
    operator text not null,
    region_id int references regions(id),
    province_id int references provinces(id),
    numbers_per_ticket smallint not null,
    number_pool smallint not null,
    has_bonus boolean default false,
    schedule jsonb not null,
    metadata jsonb default '{}'::jsonb,
    created_at timestamptz default now(),
    updated_at timestamptz default now(),
    created_by uuid references auth.users(id),
    updated_by uuid references auth.users(id)
);
create index on lottery_games(category);

-- Draw Data
create table draws (
    id uuid primary key default gen_random_uuid(),
    game_id uuid references lottery_games(id) not null,
    draw_date date not null,
    sequence integer,
    status draw_status default 'scheduled',
    started_at timestamptz,
    closed_at timestamptz,
    source_url text,
    raw_feed jsonb,
    created_at timestamptz default now(),
    updated_at timestamptz default now(),
    created_by uuid references auth.users(id),
    updated_by uuid references auth.users(id)
);
create index on draws(draw_date);
create index on draws(status);
create unique index on draws(game_id, draw_date, coalesce(sequence, 0));

create table draw_prizes (
    id uuid primary key default gen_random_uuid(),
    draw_id uuid references draws(id) on delete cascade,
    prize_level prize_level not null,
    prize_order smallint not null,
    prize_name text not null,
    reward_amount numeric(16,2) not null,
    reward_currency text default 'VND',
    winners_count integer default 0,
    created_at timestamptz default now(),
    updated_at timestamptz default now(),
    created_by uuid references auth.users(id),
    updated_by uuid references auth.users(id),
    unique (draw_id, prize_level, prize_order)
);

create table draw_results (
    id uuid primary key default gen_random_uuid(),
    prize_id uuid references draw_prizes(id) on delete cascade,
    result_numbers text[] not null,
    bonus_numbers text[],
    province_id int references provinces(id),
    created_at timestamptz default now(),
    created_by uuid references auth.users(id)
);
create index on draw_results(province_id);

-- Live Updates
create table live_events (
    id bigint primary key generated always as identity,
    draw_id uuid references draws(id) on delete cascade,
    emitted_at timestamptz default now(),
    payload jsonb not null,
    stage text not null
);
create unique index on live_events(draw_id, stage, emitted_at);

-- User Profiles
create table profiles (
    id uuid primary key references auth.users(id) on delete cascade,
    full_name text,
    locale text default 'vi-VN',
    notification_opt_in boolean default true,
    created_at timestamptz default now(),
    updated_at timestamptz default now()
);

-- Enable RLS on profiles
alter table profiles enable row level security;

-- RLS Policies for profiles
create policy "Users can view their own profile"
    on profiles for select
    using (auth.uid() = id);

create policy "Users can update their own profile"
    on profiles for update
    using (auth.uid() = id);

create policy "Users can insert their own profile"
    on profiles for insert
    with check (auth.uid() = id);

-- User Favorites
create table user_favorites (
    id uuid primary key default gen_random_uuid(),
    user_id uuid references profiles(id) on delete cascade not null,
    game_id uuid references lottery_games(id),
    province_id int references provinces(id),
    created_at timestamptz default now(),
    unique (user_id, game_id, province_id)
);

-- Enable RLS on user_favorites
alter table user_favorites enable row level security;

-- RLS Policies for user_favorites
create policy "Users can view their own favorites"
    on user_favorites for select
    using (auth.uid() = user_id);

create policy "Users can insert their own favorites"
    on user_favorites for insert
    with check (auth.uid() = user_id);

create policy "Users can delete their own favorites"
    on user_favorites for delete
    using (auth.uid() = user_id);

-- Tickets
create table tickets (
    id uuid primary key default gen_random_uuid(),
    user_id uuid references profiles(id) on delete cascade not null,
    game_id uuid references lottery_games(id) not null,
    purchase_date date,
    draw_id uuid references draws(id),
    ticket_numbers text[] not null,
    extra_numbers text[],
    source text,
    note text,
    created_at timestamptz default now(),
    updated_at timestamptz default now()
);

-- Enable RLS on tickets
alter table tickets enable row level security;

-- RLS Policies for tickets
create policy "Users can view their own tickets"
    on tickets for select
    using (auth.uid() = user_id);

create policy "Users can insert their own tickets"
    on tickets for insert
    with check (auth.uid() = user_id);

create policy "Users can update their own tickets"
    on tickets for update
    using (auth.uid() = user_id);

create policy "Users can delete their own tickets"
    on tickets for delete
    using (auth.uid() = user_id);

-- Ticket Checks
create table ticket_checks (
    id uuid primary key default gen_random_uuid(),
    ticket_id uuid references tickets(id) on delete cascade not null,
    checked_draw_id uuid references draws(id),
    status ticket_status default 'pending',
    matched_prize_id uuid references draw_prizes(id),
    matched_numbers text[],
    payout numeric(16,2),
    checked_at timestamptz default now(),
    created_at timestamptz default now()
);
create index on ticket_checks(status);

-- Enable RLS on ticket_checks
alter table ticket_checks enable row level security;

-- RLS Policies for ticket_checks (users can see checks for their own tickets)
create policy "Users can view their own ticket checks"
    on ticket_checks for select
    using (
        exists (
            select 1 from tickets
            where tickets.id = ticket_checks.ticket_id
            and tickets.user_id = auth.uid()
        )
    );

-- Statistics
create table draw_statistics_daily (
    id bigint primary key generated always as identity,
    game_id uuid references lottery_games(id),
    draw_date date not null,
    appearance_counts jsonb not null,
    hot_numbers text[],
    cold_numbers text[],
    odd_even_distribution jsonb,
    sum_distribution jsonb,
    created_at timestamptz default now(),
    unique (game_id, draw_date)
);

-- Materialized View for Number Frequency
create materialized view mv_number_frequency as
    select game_id,
           result_number,
           count(*) as hits,
           min(draw_date) as first_seen,
           max(draw_date) as last_seen
    from (
        select d.game_id,
               dr.id as result_id,
               unnest(result_numbers) as result_number,
               d.draw_date
        from draw_results dr
             join draw_prizes dp on dr.prize_id = dp.id
             join draws d on d.id = dp.draw_id
    ) s
    group by game_id, result_number;

-- Notifications
create table notification_channels (
    id serial primary key,
    code text unique not null,
    name text not null
);

create table notification_subscriptions (
    id uuid primary key default gen_random_uuid(),
    user_id uuid references profiles(id) on delete cascade not null,
    channel_id int references notification_channels(id),
    game_id uuid references lottery_games(id),
    province_id int references provinces(id),
    is_active boolean default true,
    config jsonb default '{}'::jsonb,
    created_at timestamptz default now(),
    updated_at timestamptz default now(),
    unique (user_id, channel_id, game_id, province_id)
);

-- Enable RLS on notification_subscriptions
alter table notification_subscriptions enable row level security;

-- RLS Policies for notification_subscriptions
create policy "Users can view their own subscriptions"
    on notification_subscriptions for select
    using (auth.uid() = user_id);

create policy "Users can insert their own subscriptions"
    on notification_subscriptions for insert
    with check (auth.uid() = user_id);

create policy "Users can update their own subscriptions"
    on notification_subscriptions for update
    using (auth.uid() = user_id);

create policy "Users can delete their own subscriptions"
    on notification_subscriptions for delete
    using (auth.uid() = user_id);

create table notifications (
    id uuid primary key default gen_random_uuid(),
    subscription_id uuid references notification_subscriptions(id),
    draw_id uuid references draws(id),
    sent_at timestamptz,
    payload jsonb not null,
    status text default 'queued',
    error text,
    created_at timestamptz default now()
);

-- Import and Audit
create table import_jobs (
    id uuid primary key default gen_random_uuid(),
    source text not null,
    status text default 'pending',
    requested_at timestamptz default now(),
    started_at timestamptz,
    finished_at timestamptz,
    file_path text,
    message text,
    created_by uuid references profiles(id) not null,
    created_at timestamptz default now()
);

-- Enable RLS on import_jobs
alter table import_jobs enable row level security;

-- RLS Policies for import_jobs
create policy "Users can view their own import jobs"
    on import_jobs for select
    using (auth.uid() = created_by);

create policy "Users can insert their own import jobs"
    on import_jobs for insert
    with check (auth.uid() = created_by);

create table change_log (
    id bigint primary key generated always as identity,
    table_name text not null,
    record_id uuid not null,
    action text not null,
    payload jsonb,
    occurred_at timestamptz default now(),
    triggered_by uuid references profiles(id)
);

-- Function to update updated_at timestamp
create or replace function update_updated_at_column()
returns trigger as $$
begin
    new.updated_at = now();
    return new;
end;
$$ language plpgsql;

-- Add triggers for updated_at on tables
create trigger update_regions_updated_at before update on regions
    for each row execute function update_updated_at_column();

create trigger update_provinces_updated_at before update on provinces
    for each row execute function update_updated_at_column();

create trigger update_lottery_games_updated_at before update on lottery_games
    for each row execute function update_updated_at_column();

create trigger update_draws_updated_at before update on draws
    for each row execute function update_updated_at_column();

create trigger update_draw_prizes_updated_at before update on draw_prizes
    for each row execute function update_updated_at_column();

create trigger update_profiles_updated_at before update on profiles
    for each row execute function update_updated_at_column();

create trigger update_tickets_updated_at before update on tickets
    for each row execute function update_updated_at_column();

create trigger update_notification_subscriptions_updated_at before update on notification_subscriptions
    for each row execute function update_updated_at_column();

-- Enable realtime for specified tables
alter publication supabase_realtime add table draws;
alter publication supabase_realtime add table draw_prizes;
alter publication supabase_realtime add table draw_results;
alter publication supabase_realtime add table live_events;

-- Public read access for reference and draw data
create policy "Anyone can view regions"
    on regions for select
    using (true);

create policy "Anyone can view provinces"
    on provinces for select
    using (true);

create policy "Anyone can view lottery games"
    on lottery_games for select
    using (true);

create policy "Anyone can view draws"
    on draws for select
    using (true);

create policy "Anyone can view draw prizes"
    on draw_prizes for select
    using (true);

create policy "Anyone can view draw results"
    on draw_results for select
    using (true);

create policy "Anyone can view live events"
    on live_events for select
    using (true);

create policy "Anyone can view draw statistics"
    on draw_statistics_daily for select
    using (true);

create policy "Anyone can view notification channels"
    on notification_channels for select
    using (true);

-- Enable RLS on public reference tables
alter table regions enable row level security;
alter table provinces enable row level security;
alter table lottery_games enable row level security;
alter table draws enable row level security;
alter table draw_prizes enable row level security;
alter table draw_results enable row level security;
alter table live_events enable row level security;
alter table draw_statistics_daily enable row level security;
alter table notification_channels enable row level security;