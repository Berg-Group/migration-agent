#!/bin/bash
# Run validation on just the company_identities table
export TABLE_PREFIXES="company_identities"
npm run qa:quiet 