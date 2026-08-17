import { describe, expect, it } from 'vitest'

import type { BillingEntitlement } from '@/api/billing'
import {
  entitlementReplacementGroups,
  entitlementsWillReplaceExisting,
  isPlanEntitlementReplacementCandidate,
} from '../billingEntitlements'

function policy(replacementGroup?: string): BillingEntitlement {
  return {
    type: 'usage_policy',
    replacement_group: replacementGroup,
    rules: [{
      metric: 'request_count',
      window: { kind: 'calendar_week' },
      limit: 1_000,
    }],
  }
}

describe('billing entitlement replacement', () => {
  it('matches the backend replacement query for entitlement record state and expiry', () => {
    const now = Date.parse('2026-08-14T12:00:00Z')

    expect(isPlanEntitlementReplacementCandidate({
      status: 'active',
      expires_at: '2026-08-14T12:00:01Z',
    }, now)).toBe(true)
    expect(isPlanEntitlementReplacementCandidate({
      status: 'active',
      expires_at: '2026-08-14T12:00:00Z',
    }, now)).toBe(false)
    expect(isPlanEntitlementReplacementCandidate({
      status: 'replaced',
      expires_at: '2026-08-15T12:00:00Z',
    }, now)).toBe(false)
    expect(isPlanEntitlementReplacementCandidate({
      status: 'active',
      expires_at: null,
    }, now)).toBe(false)
    expect(isPlanEntitlementReplacementCandidate({
      status: 'active',
      expires_at: 'invalid',
    }, now)).toBe(false)
  })

  it('keeps usage policies stackable when neither package declares a group', () => {
    expect(entitlementsWillReplaceExisting([policy()], [policy()])).toBe(false)
    expect(entitlementsWillReplaceExisting(undefined, [policy()])).toBe(false)
    expect(entitlementsWillReplaceExisting([policy()], undefined)).toBe(false)
  })

  it('preserves legacy whole-package replacement for matching entitlement types', () => {
    const incoming: BillingEntitlement[] = [
      { type: 'daily_quota', daily_quota_usd: 20 },
      policy('new-policy-group'),
    ]
    const existing: BillingEntitlement[] = [
      { type: 'daily_quota', daily_quota_usd: 10 },
      policy('old-policy-group'),
    ]

    expect(entitlementsWillReplaceExisting(incoming, existing)).toBe(true)
    expect(entitlementsWillReplaceExisting(
      [{ type: 'membership_group', grant_user_groups: ['pro'] }],
      [{ type: 'membership_group', grant_user_groups: ['basic'] }],
    )).toBe(true)
    expect(entitlementsWillReplaceExisting(
      [{ type: 'daily_quota', daily_quota_usd: 20 }],
      [{ type: 'membership_group', grant_user_groups: ['basic'] }],
    )).toBe(false)
  })

  it('matches explicit groups across entitlement types after trimming whitespace', () => {
    expect(entitlementsWillReplaceExisting(
      [policy(' pro-tier ')],
      [{ type: 'wallet_credit', replacement_group: 'pro-tier', amount_usd: 10 }],
    )).toBe(true)
  })

  it('uses exact case-sensitive group identity like the backend', () => {
    expect(entitlementsWillReplaceExisting([policy('Pro-Tier')], [policy('pro-tier')])).toBe(false)
  })

  it('normalizes, removes blanks, and deduplicates groups in display order', () => {
    expect(entitlementReplacementGroups([
      policy(' pro-tier '),
      policy(''),
      policy('pro-tier'),
      policy('team-tier'),
    ])).toEqual(['pro-tier', 'team-tier'])
    expect(entitlementReplacementGroups(undefined)).toEqual([])
  })
})
