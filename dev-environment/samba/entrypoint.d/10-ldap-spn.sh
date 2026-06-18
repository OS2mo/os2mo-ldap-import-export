#!/bin/sh
# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
#
# Register the LDAP service principal on the domain controller account, so
# Kerberos clients can obtain a ticket for ldap/dc.magenta.dk and bind with SASL
# GSSAPI. A full Samba AD DC does this through samba_spnupdate, which belongs to
# the dns service that smblds does not run. Idempotent across restarts.
set -eu
samba-tool spn list 'DC$' | grep -q 'ldap/dc.magenta.dk' \
  || samba-tool spn add 'ldap/dc.magenta.dk' 'DC$'
