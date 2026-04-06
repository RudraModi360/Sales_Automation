# Create Campaign API Documentation

## Endpoint Overview

**POST** `/api/v2/campaigns`

Create a new email campaign for sending outreach emails to a list of recipients.

---

## Prerequisites & Requirements

- **API Authentication**: Bearer token required in `Authorization` header
- **Required Scopes**: One of the following:
  - `campaigns:create`
  - `campaigns:all`
  - `all:create`
  - `all:all`

---

## Request Schema

### Basic Information

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `name` | string | ✓ | Name of the campaign | `My First Campaign` |
| `pl_value` | number \| null | | Value of every positive lead (monetary amount) | `100` |
| `is_evergreen` | boolean \| null | | Whether the campaign runs indefinitely | `false` |
| `owned_by` | string (UUID) \| null | | Owner/User ID assigned to this campaign | `019cc043-aee6-7ae9-a545-d7e131be967e` |
| `ai_sdr_id` | string (UUID) \| null | | AI SDR ID that created this campaign | `019cc043-aee6-7ae9-a545-d7e2b38afb00` |

### Campaign Scheduling

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `campaign_schedule` | object | ✓ | Schedule configuration for when emails are sent |
| `campaign_schedule.start_date` | string (date) \| null | | Campaign start date in `YYYY-MM-DD` format (uses campaign timezone) |
| `campaign_schedule.end_date` | string (date) \| null | | Campaign end date in `YYYY-MM-DD` format (uses campaign timezone) |
| `campaign_schedule.schedules` | array | ✓ | Array of sending schedules (at least 1 required) |

**Schedule Item Properties** (`campaign_schedule.schedules[*]`):

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `name` | string | ✓ | Name of the schedule | `My Schedule` |
| `timing.from` | string | ✓ | Start time in 24-hour format `HH:MM` | `09:00` |
| `timing.to` | string | ✓ | End time in 24-hour format `HH:MM` | `17:00` |
| `days` | object | ✓ | Days of week (min 1 day required). Keys `0-6` (Sunday=0), values are boolean | `{"0": true, "1": true, "2": true}` |
| `timezone` | string (enum) | ✓ | Timezone for this schedule (see [Supported Timezones](#supported-timezones)) | `Etc/GMT+12` |

### Email Sequences (Email Copy)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sequences` | array | ✓ | Array of email sequences (only first element is used) |
| `sequences[0].steps` | array | ✓ | Email steps within the sequence (at least 1 required) |

**Step Item Properties** (`sequences[0].steps[*]`):

| Field | Type | Required | Description | Default | Example |
|-------|------|----------|-------------|---------|---------|
| `type` | string (enum) | ✓ | Type of step (currently only `email` supported) | - | `email` |
| `delay` | number | ✓ | Delay before sending NEXT email (unit: `delay_unit`) | - | `2` |
| `delay_unit` | string (enum) | | Unit for delay: `minutes`, `hours`, `days` | `days` | `days` |
| `pre_delay` | number | | Delay before first email in subsequence (subsequences only) | - | `2` |
| `pre_delay_unit` | string (enum) | | Unit for pre_delay: `minutes`, `hours`, `days` (subsequences only) | `days` | `days` |
| `variants` | array | ✓ | Email content variants (at least 1 required) | - | - |

**Variant Item Properties** (`sequences[0].steps[*].variants[*]`):

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `subject` | string | ✓ | Email subject line (supports template variables) | `Hello {{firstName}}` |
| `body` | string | ✓ | Email body (supports template variables) | `Hey {{firstName}},\n\nI hope you are doing well.` |
| `v_disabled` | boolean | | Disable this variant from sending | `true` |

### Sending Configuration

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `email_list` | array | | List of sending email accounts (account IDs/emails) | `["john@doe.com", "jane@doe.com"]` |
| `email_tag_list` | array | | List of email account tag UUIDs for sending | `["019cc043-aee6-7ae9-a545-d7df85a583a2"]` |
| `daily_limit` | number \| null | | Maximum emails sent per day per account | `100` |
| `daily_max_leads` | number \| null | | Maximum NEW leads to contact per day | `100` |
| `email_gap` | number \| null | | Minimum gap between emails sent (in minutes) | `10` |
| `random_wait_max` | number \| null | | Maximum random wait time (in minutes) | `10` |
| `prioritize_new_leads` | boolean \| null | | Prioritize newly added leads in sending order | `false` |

### Email Content Options

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `text_only` | boolean \| null | | Send emails as text-only (no HTML) | `false` |
| `first_email_text_only` | boolean \| null | | Send first email as text-only, others as HTML | `false` |
| `cc_list` | array | | Email addresses to CC on all emails | `["manager@doe.com"]` |
| `bcc_list` | array | | Email addresses to BCC on all emails | `["archive@doe.com"]` |

### Tracking & Campaign Behavior

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `link_tracking` | boolean \| null | | Track clicks on links in emails | `true` |
| `open_tracking` | boolean \| null | | Track email opens | `true` |
| `stop_on_reply` | boolean \| null | | Stop campaign for lead when they reply | `false` |
| `stop_on_auto_reply` | boolean \| null | | Stop campaign for lead on auto-reply detection | `false` |
| `stop_for_company` | boolean \| null | | Stop entire company domain when ANY lead replies | `false` |
| `insert_unsubscribe_header` | boolean \| null | | Add unsubscribe header to emails (RFC 8058) | `false` |
| `allow_risky_contacts` | boolean \| null | | Allow sending to potentially risky/invalid emails | `false` |
| `disable_bounce_protect` | boolean \| null | | Disable bounce protection checks | `false` |
| `match_lead_esp` | boolean \| null | | Match lead's email provider with sending account provider | `false` |

### Advanced Targeting

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `auto_variant_select` | object \| null | | Auto-select best performing variant based on metric |
| `auto_variant_select.trigger` | string (enum) | | Trigger metric: `reply_rate`, `click_rate`, `open_rate` |
| `limit_emails_per_company_override` | object \| null | | Override workspace-wide company email limits for this campaign |
| `limit_emails_per_company_override.mode` | string (enum) | ✓ | Mode: `custom` or `disabled` |
| `limit_emails_per_company_override.daily_limit` | number | | Max emails per company per day (min: 1) |
| `limit_emails_per_company_override.scope` | string (enum) | | Scope: `per_campaign` or `across_workspace` |
| `provider_routing_rules` | array | | Route emails based on recipient/sender ESP |

**Provider Routing Rule Properties** (`provider_routing_rules[*]`):

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `action` | string (enum) | ✓ | Action: `send` or `do_not_send` |
| `recipient_esp` | array (enum) | ✓ | Recipient email providers: `all`, `google`, `outlook`, `other` |
| `sender_esp` | array (enum) | ✓ | Sender email providers: `all`, `google`, `outlook`, `other` |

---

## Response Schema

### Campaign Resource (Success 200)

| Field | Type | Read-Only | Description |
|-------|------|:---------:|-------------|
| `id` | string (UUID) | ✓ | Unique identifier for the campaign |
| `name` | string | | Name of the campaign |
| `pl_value` | number \| null | | Value of every positive lead |
| `is_evergreen` | boolean \| null | | Whether the campaign runs indefinitely |
| `status` | number (enum) | ✓ | [Campaign status](#campaign-status-codes) |
| `not_sending_status` | number (enum) \| null | ✓ | [Not-sending reason](#not-sending-status-codes) |
| `campaign_schedule` | object | | Schedule configuration (same as request) |
| `sequences` | array | | Email sequences (same as request) |
| `email_list` | array | | Sending email accounts |
| `email_tag_list` | array | | Sending email account tags |
| `daily_limit` | number \| null | | Daily sending limit |
| `daily_max_leads` | number \| null | | Daily max new leads |
| `email_gap` | number \| null | | Gap between emails (minutes) |
| `random_wait_max` | number \| null | | Max random wait (minutes) |
| `text_only` | boolean \| null | | Text-only sending |
| `first_email_text_only` | boolean \| null | | First email text-only |
| `link_tracking` | boolean \| null | | Link tracking enabled |
| `open_tracking` | boolean \| null | | Open tracking enabled |
| `stop_on_reply` | boolean \| null | | Stop on reply enabled |
| `stop_on_auto_reply` | boolean \| null | | Stop on auto-reply enabled |
| `stop_for_company` | boolean \| null | | Stop entire company on reply |
| `prioritize_new_leads` | boolean \| null | | Prioritize new leads |
| `allow_risky_contacts` | boolean \| null | | Allow risky contacts |
| `disable_bounce_protect` | boolean \| null | | Bounce protection disabled |
| `insert_unsubscribe_header` | boolean \| null | | Unsubscribe header inserted |
| `match_lead_esp` | boolean \| null | | Match lead ESP enabled |
| `auto_variant_select` | object \| null | | Auto variant selection config |
| `limit_emails_per_company_override` | object \| null | | Company email limit override |
| `provider_routing_rules` | array | | Provider routing rules |
| `cc_list` | array | | CC email addresses |
| `bcc_list` | array | | BCC email addresses |
| `owned_by` | string (UUID) \| null | | Owner/User ID |
| `ai_sdr_id` | string (UUID) \| null | | AI SDR creator ID |
| `organization` | string (UUID) \| null | ✓ | Organization ID |
| `core_variables` | object \| null | ✓ | Campaign core variables (system-set) |
| `custom_variables` | object \| null | ✓ | Campaign custom variables (system-set) |
| `timestamp_created` | string (ISO 8601) | ✓ | Campaign creation timestamp |
| `timestamp_updated` | string (ISO 8601) | ✓ | Campaign last update timestamp |

### Campaign Status Codes

| Code | Status | Description |
|------|--------|-------------|
| `0` | Draft | Campaign created but not started |
| `1` | Active | Campaign is currently running |
| `2` | Paused | Campaign is paused |
| `3` | Completed | Campaign has finished |
| `4` | Running Subsequences | Campaign has active follow-up sequences |
| `-1` | Accounts Unhealthy | Sending accounts have issues |
| `-2` | Bounce Protect | Campaign paused due to bounce protection |
| `-99` | Account Suspended | Sending account is suspended |

### Not-Sending Status Codes

| Code | Reason |
|------|--------|
| `1` | Campaign is outside its sending schedule window |
| `2` | Waiting for leads to process |
| `3` | Campaign has reached daily sending limit |
| `4` | All sending accounts have reached their daily limit |
| `99` | Campaign stopped due to error (contact support) |

---

## HTTP Status Codes

### 200 OK
Campaign successfully created. Response body contains the full Campaign resource.

### 400 Bad Request
Invalid request body (missing required fields or invalid field values).

**Response:**
```json
{
  "statusCode": 400,
  "error": "Bad Request",
  "message": "body must have required property 'name'"
}
```

### 401 Unauthorized
Missing or invalid API key, or API key has been revoked.

**Response:**
```json
{
  "statusCode": 401,
  "error": "Unauthorized",
  "message": "Missing Authorization header"
}
```

### 429 Too Many Requests
Rate limit exceeded. Please retry after waiting.

**Response:**
```json
{
  "statusCode": 429,
  "error": "Too Many Requests",
  "message": "Rate limit exceeded"
}
```

---

## Supported Timezones

The following timezones are supported for `campaign_schedule.schedules[*].timezone`:

**Americas:**
- `America/Anchorage`, `America/Boise`, `America/Chicago`, `America/Denver`, `America/Detroit`, `America/Los_Angeles`, `America/New_York`, `America/Toronto`, `America/Vancouver`, `America/Mexico_City`, `America/Panama`, `America/Bogota`, `America/Lima`, `America/Caracas`, `America/Sao_Paulo`, `America/Argentina/Buenos_Aires`, `America/Montevideo`, `America/Godthab`, `America/St_Johns`, `America/Noronha`, `America/Danmarkshavn`, `America/Belize`, `America/Bahia_Banderas`, `America/Regina`, `America/Anguilla`, `America/Santiago`, `America/Campo_Grande`, `America/Araguaina`, `America/Creston`, `America/Chihuahua`, `America/Dawson`, `America/Glace_Bay`, `America/Indiana/Marengo`, `America/Asuncion`, `America/Scoresbysund`, `Atlantic/Cape_Verde`

**Europe & Africa:**
- `Europe/London`, `Europe/Dublin`, `Europe/Paris`, `Europe/Berlin`, `Europe/Amsterdam`, `Europe/Brussels`, `Europe/Vienna`, `Europe/Prague`, `Europe/Budapest`, `Europe/Warsaw`, `Europe/Istanbul`, `Europe/Moscow`, `Europe/Athens`, `Europe/Helsinki`, `Europe/Belgrade`, `Europe/Bucharest`, `Europe/Kaliningrad`, `Europe/Kirov`, `Europe/Astrakhan`, `Africa/Casablanca`, `Africa/Algiers`, `Africa/Cairo`, `Africa/Johannesburg`, `Africa/Lagos`, `Africa/Nairobi`, `Africa/Windhoek`, `Africa/Tripoli`, `Africa/Addis_Ababa`, `Africa/Abidjan`, `Africa/Blantyre`, `Africa/Ceuta`, `Arctic/Longyearbyen`, `Atlantic/Canary`, `Europe/Isle_of_Man`, `Europe/Sarajevo`

**Middle East & Asia:**
- `Asia/Dubai`, `Asia/Baghdad`, `Asia/Tehran`, `Asia/Jerusalem`, `Asia/Amman`, `Asia/Beirut`, `Asia/Damascus`, `Asia/Nicosia`, `Asia/Yerevan`, `Asia/Baku`, `Asia/Tbilisi`, `Asia/Yekaterinburg`, `Asia/Karachi`, `Asia/Kolkata`, `Asia/Colombo`, `Asia/Kathmandu`, `Asia/Dhaka`, `Asia/Rangoon`, `Asia/Bangkok`, `Asia/Ho_Chi_Minh`, `Asia/Hong_Kong`, `Asia/Shanghai`, `Asia/Singapore`, `Asia/Taipei`, `Asia/Tokyo`, `Asia/Seoul`, `Asia/Pyongyang`, `Asia/Krasnoyarsk`, `Asia/Novokuznetsk`, `Asia/Irkutsk`, `Asia/Choibalsan`, `Asia/Chita`, `Asia/Sakhalin`, `Asia/Dili`, `Asia/Anadyr`, `Asia/Kamchatka`, `Asia/Kabul`, `Asia/Aden`, `Indian/Mahe`

**Oceania & Antarctica:**
- `Australia/Perth`, `Australia/Adelaide`, `Australia/Darwin`, `Australia/Brisbane`, `Australia/Hobart`, `Australia/Melbourne`, `Australia/Sydney`, `Pacific/Auckland`, `Pacific/Fiji`, `Pacific/Apia`, `Etc/GMT+12`, `Etc/GMT+11`, `Etc/GMT+10`, `Etc/GMT-12`, `Etc/GMT-13`, `Antarctica/Mawson`, `Antarctica/Vostok`, `Antarctica/Davis`, `Antarctica/DumontDUrville`, `Antarctica/Macquarie`, `Etc/GMT+12` (UTC-12), `Etc/GMT-12` (UTC+12)

---

## Minimal Example

```json
{
  "name": "My First Campaign",
  "campaign_schedule": {
    "schedules": [
      {
        "name": "Business Hours",
        "timing": {
          "from": "09:00",
          "to": "17:00"
        },
        "days": {
          "1": true,
          "2": true,
          "3": true,
          "4": true,
          "5": true,
          "0": false,
          "6": false
        },
        "timezone": "America/New_York"
      }
    ]
  },
  "sequences": [
    {
      "steps": [
        {
          "type": "email",
          "delay": 2,
          "variants": [
            {
              "subject": "Hey {{firstName}}",
              "body": "Hi {{firstName}},\n\nInterested in learning more about our product?\n\nBest regards"
            }
          ]
        }
      ]
    }
  ],
  "email_list": ["sender@company.com"],
  "daily_limit": 100
}
```

---

## Complete Example

```json
{
  "name": "Q2 Product Launch Campaign",
  "pl_value": 250,
  "is_evergreen": false,
  "campaign_schedule": {
    "start_date": "2026-04-15",
    "end_date": "2026-06-30",
    "schedules": [
      {
        "name": "Morning Batch",
        "timing": {
          "from": "09:00",
          "to": "12:00"
        },
        "days": {
          "1": true,
          "2": true,
          "3": true,
          "4": true,
          "5": true,
          "0": false,
          "6": false
        },
        "timezone": "America/New_York"
      }
    ]
  },
  "sequences": [
    {
      "steps": [
        {
          "type": "email",
          "delay": 0,
          "variants": [
            {
              "subject": "Quick question for {{firstName}} at {{companyName}}",
              "body": "Hi {{firstName}},\n\nI saw {{companyName}} is scaling sales operations...\n\nWould a quick 15-minute call work?\n\nBest,\nJohn"
            }
          ]
        },
        {
          "type": "email",
          "delay": 3,
          "variants": [
            {
              "subject": "Following up - {{firstName}}",
              "body": "Hi {{firstName}},\n\nJust following up on my previous email.\n\nLet me know if you're interested!\n\nBest"
            }
          ]
        }
      ]
    }
  ],
  "email_list": ["sales1@company.com", "sales2@company.com"],
  "email_tag_list": ["019cc043-aee6-7ae9-a545-d7df85a583a2"],
  "daily_limit": 150,
  "daily_max_leads": 100,
  "email_gap": 5,
  "random_wait_max": 10,
  "link_tracking": true,
  "open_tracking": true,
  "stop_on_reply": false,
  "stop_on_auto_reply": true,
  "insert_unsubscribe_header": true,
  "cc_list": ["manager@company.com"],
  "prioritize_new_leads": true,
  "auto_variant_select": {
    "trigger": "click_rate"
  },
  "owned_by": "019cc043-aee6-7ae9-a545-d7e131be967e"
}
```

---

## Notes

- **Template Variables**: Supported placeholders in email subject/body: `{{firstName}}`, `{{companyName}}`, `{{email}}`, and any custom variables defined in the campaign
- **Sequences Array**: Only the first element in the `sequences` array is used; provide exactly one sequence object
- **Days Object**: Provide at least one day (`0`=Sunday through `6`=Saturday)
- **Recurring Campaigns**: Combine `start_date`, `end_date`, and `is_evergreen` to control campaign duration
- **Rate Limiting**: API requests are rate-limited; check response headers for limit status
