-- Fix Function Search Path for update_updated_at_column
create or replace function update_updated_at_column()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
    new.updated_at = now();
    return new;
end;
$$;

-- Enable RLS on notifications table
alter table notifications enable row level security;

-- RLS Policies for notifications (users can see notifications for their own subscriptions)
create policy "Users can view their own notifications"
    on notifications for select
    using (
        exists (
            select 1 from notification_subscriptions
            where notification_subscriptions.id = notifications.subscription_id
            and notification_subscriptions.user_id = auth.uid()
        )
    );

-- Enable RLS on change_log table
alter table change_log enable row level security;

-- RLS Policy for change_log (only admins or the user who triggered can view)
create policy "Users can view their own change logs"
    on change_log for select
    using (auth.uid() = triggered_by);

-- Hide materialized view from public API by revoking access
revoke all on mv_number_frequency from anon, authenticated;
grant select on mv_number_frequency to service_role;