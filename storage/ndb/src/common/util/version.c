/*
   Copyright (C) 2003 MySQL AB
    All rights reserved. Use is subject to license terms.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#include <ndb_global.h>
#include <ndb_version.h>
#include <version.h>
#include <basestring_vsnprintf.h>
#include <NdbOut.hpp>

Uint32 ndbGetMajor(Uint32 version) {
  return (version >> 16) & 0xFF;
}

Uint32 ndbGetMinor(Uint32 version) {
  return (version >> 8) & 0xFF;
}

Uint32 ndbGetBuild(Uint32 version) {
  return (version >> 0) & 0xFF;
}

Uint32 ndbMakeVersion(Uint32 major, Uint32 minor, Uint32 build) {
  return NDB_MAKE_VERSION(major, minor, build);
  
}

const char * ndbGetOwnVersionString()
{
  static char ndb_version_string_buf[NDB_VERSION_STRING_BUF_SZ];
  return ndbGetVersionString(NDB_VERSION, NDB_MYSQL_VERSION_D, NDB_VERSION_STATUS,
                             ndb_version_string_buf,
                             sizeof(ndb_version_string_buf));
}

const char * ndbGetVersionString(Uint32 version, Uint32 mysql_version, const char * status,
                                 char *buf, unsigned sz)
{
  char tmp[NDB_VERSION_STRING_BUF_SZ];
  if (status && status[0] != 0)
    basestring_snprintf(tmp, sizeof(tmp), "%s", status);
  else
    tmp[0] = 0;

  if (mysql_version)
  {
    basestring_snprintf(buf, sz, "mysql-%d.%d.%d ndb-%d.%d.%d%s",
			getMajor(mysql_version),
			getMinor(mysql_version),
			getBuild(mysql_version),
			getMajor(version),
			getMinor(version),
			getBuild(version),
			tmp);
  }
  else
  {
    basestring_snprintf(buf, sz, "ndb-%d.%d.%d%s",
			getMajor(version),
			getMinor(version),
			getBuild(version),
			tmp);
  }
  return buf;
}

typedef enum {
  UG_Null,
  UG_Range,
  UG_Exact
} UG_MatchType;

struct NdbUpGradeCompatible {
  Uint32 ownVersion;
  Uint32 otherVersion;
  UG_MatchType matchType;
};

struct NdbUpGradeCompatible ndbCompatibleTable_full[] = {
  { MAKE_VERSION(7,0,NDB_VERSION_BUILD), MAKE_VERSION(7,0,0), UG_Range },
  { MAKE_VERSION(7,0,NDB_VERSION_BUILD), MAKE_VERSION(6,4,0), UG_Range },
  /* Can only upgrade to 6.4.X from versions >= 6.3.17 due to change
   * in Transporter maximum sent message size
   */
  { MAKE_VERSION(7,0,NDB_VERSION_BUILD), NDBD_MAX_RECVBYTESIZE_32K, UG_Range },
  { MAKE_VERSION(6,3,NDB_VERSION_BUILD), MAKE_VERSION(6,2,1), UG_Range },

  { MAKE_VERSION(6,2,NDB_VERSION_BUILD), MAKE_VERSION(6,2,1), UG_Range },
  { MAKE_VERSION(6,2,0), MAKE_VERSION(6,2,0), UG_Range},

  { MAKE_VERSION(6,2,NDB_VERSION_BUILD), MAKE_VERSION(6,1,19), UG_Range },
  { MAKE_VERSION(6,1,NDB_VERSION_BUILD), MAKE_VERSION(6,1,6), UG_Range},
  /* var page reference 32bit->64bit making 6.1.6 not backwards compatible */
  /* ndb_apply_status table changed, and no compatability code written */
  { MAKE_VERSION(6,1,4), MAKE_VERSION(6,1,2), UG_Range},
  { MAKE_VERSION(5,1,NDB_VERSION_BUILD), MAKE_VERSION(5,1,0), UG_Range},

  { MAKE_VERSION(5,1,NDB_VERSION_BUILD), MAKE_VERSION(5,1,18), UG_Range},
  { MAKE_VERSION(5,1,17), MAKE_VERSION(5,1,0), UG_Range},

  { MAKE_VERSION(5,0,NDB_VERSION_BUILD), MAKE_VERSION(5,0,12), UG_Range},
  { MAKE_VERSION(5,0,11), MAKE_VERSION(5,0,2), UG_Range},
  { MAKE_VERSION(4,1,NDB_VERSION_BUILD), MAKE_VERSION(4,1,15), UG_Range },
  { MAKE_VERSION(4,1,14), MAKE_VERSION(4,1,10), UG_Range },
  { MAKE_VERSION(4,1,10), MAKE_VERSION(4,1,9), UG_Exact },
  { MAKE_VERSION(4,1,9), MAKE_VERSION(4,1,8), UG_Exact },
  { MAKE_VERSION(3,5,2), MAKE_VERSION(3,5,1), UG_Exact },
  { 0, 0, UG_Null }
};

struct NdbUpGradeCompatible ndbCompatibleTable_upgrade[] = {
  { MAKE_VERSION(5,0,12), MAKE_VERSION(5,0,11), UG_Exact },
  { MAKE_VERSION(5,0,2), MAKE_VERSION(4,1,8), UG_Exact },
  { MAKE_VERSION(4,1,15), MAKE_VERSION(4,1,14), UG_Exact },
  { MAKE_VERSION(3,5,4), MAKE_VERSION(3,5,3), UG_Exact },
  { 0, 0, UG_Null }
};

void ndbPrintVersion()
{
  printf("Version: %u.%u.%u\n",
	 getMajor(ndbGetOwnVersion()),
	 getMinor(ndbGetOwnVersion()),
	 getBuild(ndbGetOwnVersion()));
}

Uint32
ndbGetOwnVersion()
{
  return NDB_VERSION_D;
}

int
ndbSearchUpgradeCompatibleTable(Uint32 ownVersion, Uint32 otherVersion,
				struct NdbUpGradeCompatible table[])
{
  int i;
  for (i = 0; table[i].ownVersion != 0 && table[i].otherVersion != 0; i++) {
    if (table[i].ownVersion == ownVersion ||
	table[i].ownVersion == (Uint32) ~0) {
      switch (table[i].matchType) {
      case UG_Range:
	if (otherVersion >= table[i].otherVersion){
	  return 1;
	}
	break;
      case UG_Exact:
	if (otherVersion == table[i].otherVersion){
	  return 1;
	}
	break;
      default:
	break;
      }
    }
  }
  return 0;
}

int
ndbCompatible(Uint32 ownVersion, Uint32 otherVersion, struct NdbUpGradeCompatible table[])
{
  if (otherVersion >= ownVersion) {
    return 1;
  }
  return ndbSearchUpgradeCompatibleTable(ownVersion, otherVersion, table);
}

int
ndbCompatible_full(Uint32 ownVersion, Uint32 otherVersion)
{
  return ndbCompatible(ownVersion, otherVersion, ndbCompatibleTable_full);
}

int
ndbCompatible_upgrade(Uint32 ownVersion, Uint32 otherVersion)
{
  if (ndbCompatible_full(ownVersion, otherVersion))
    return 1;
  return ndbCompatible(ownVersion, otherVersion, ndbCompatibleTable_upgrade);
}

int
ndbCompatible_mgmt_ndb(Uint32 ownVersion, Uint32 otherVersion)
{
  return ndbCompatible_upgrade(ownVersion, otherVersion);
}

int
ndbCompatible_mgmt_api(Uint32 ownVersion, Uint32 otherVersion)
{
  return ndbCompatible_upgrade(ownVersion, otherVersion);
}

int
ndbCompatible_ndb_mgmt(Uint32 ownVersion, Uint32 otherVersion)
{
  return ndbCompatible_full(ownVersion, otherVersion);
}

int
ndbCompatible_api_mgmt(Uint32 ownVersion, Uint32 otherVersion)
{
  return ndbCompatible_full(ownVersion, otherVersion);
}

int
ndbCompatible_api_ndb(Uint32 ownVersion, Uint32 otherVersion)
{
  return ndbCompatible_full(ownVersion, otherVersion);
}

int
ndbCompatible_ndb_api(Uint32 ownVersion, Uint32 otherVersion)
{
  return ndbCompatible_upgrade(ownVersion, otherVersion);
}

int
ndbCompatible_ndb_ndb(Uint32 ownVersion, Uint32 otherVersion)
{
  return ndbCompatible_upgrade(ownVersion, otherVersion);
}


void
ndbPrintCompatibleTable(struct NdbUpGradeCompatible table[])
{
  int i;
  printf("ownVersion, matchType, otherVersion\n");
  for (i = 0; table[i].ownVersion != 0 && table[i].otherVersion != 0; i++) {

    printf("%u.%u.%u, ",
           getMajor(table[i].ownVersion),
           getMinor(table[i].ownVersion),
           getBuild(table[i].ownVersion));
    switch (table[i].matchType) {
    case UG_Range:
      printf("Range");
      break;
    case UG_Exact:
      printf("Exact");
      break;
    default:
      break;
    }
    printf(", %u.%u.%u\n",
           getMajor(table[i].otherVersion),
           getMinor(table[i].otherVersion),
           getBuild(table[i].otherVersion));

  }
  printf("\n");
}


void
ndbPrintFullyCompatibleTable(void){
  printf("ndbCompatibleTable_full\n");
  ndbPrintCompatibleTable(ndbCompatibleTable_full);
}


void
ndbPrintUpgradeCompatibleTable(void){
  printf("ndbCompatibleTable_upgrade\n");
  ndbPrintCompatibleTable(ndbCompatibleTable_upgrade);
}
