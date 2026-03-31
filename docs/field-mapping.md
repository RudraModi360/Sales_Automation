# Summary Field Mapping

This document describes how summary fields are produced from overview fields.

## Person Fields
- full_name: person.name, fallback first_name + last_name
- first_name: person.first_name
- last_name: person.last_name
- email: person.email
- job_title: person.title
- headline: person.headline
- seniority: person.seniority
- function: person.functions
- subdepartment: person.subdepartments
- person_linkedin: person.linkedin_url
- person_twitter: person.twitter_url
- person_location: person.formatted_address, fallback city/state/country
- timezone: person.time_zone

## Company Fields
- company_name: person.organization.name
- company_description: person.organization.short_description
- company_website: person.organization.website_url
- company_domain: person.organization.primary_domain
- company_linkedin: person.organization.linkedin_url
- company_twitter: person.organization.twitter_url
- company_facebook: person.organization.facebook_url
- company_phone: primary_phone.number, fallback phone, fallback sanitized variants
- company_industry: person.organization.industry
- company_industries: person.organization.industries
- company_secondary_industries: person.organization.secondary_industries
- company_estimated_employees: person.organization.estimated_num_employees
- company_revenue: organization_revenue_printed, annual_revenue_printed, organization_revenue
- company_founded_year: person.organization.founded_year
- company_languages: person.organization.languages
- company_address: person.organization.raw_address, fallback street_address
- company_city: person.organization.city
- company_state: person.organization.state
- company_country: person.organization.country

## Constant Fields
- source: apollo

## Finalization Rules
- Drop columns that are completely empty.
- Deduplicate using available business keys in this order: email, full_name, company_name.
- Sort by email with nulls last.
