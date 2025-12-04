#!/bin/bash

DEB_AMD64_PACKAGE_BASENAME=
DEB_ARM64_PACKAGE_BASENAME=
RPM_AMD64_PACKAGE_BASENAME=
RPM_ARM64_PACKAGE_BASENAME=

NUM_DEB_PKGS=$( (dpkg -l 2>/dev/null) | wc -l )
NUM_RPM_PKGS=$( (rpm -qa 2>/dev/null) | wc -l )

PKG_TYPE=$( \
    if [ "${NUM_DEB_PKGS}" -ne 0 ]; then \
		echo "deb"; \
	elif [ "${NUM_RPM_PKGS}" -ne 0 ]; then \
		echo "rpm"; \
	else \
		echo "<unknown>"; \
	fi)

SCRIPT_VARIANT=$(basename $0)

SCRIPT_DIR=$(dirname $0)

UNAMEM=$(uname -m)

if [ "${UNAMEM}" = "x86_64" ]; then
	if [ "${PKG_TYPE}" = "deb" ]; then
		if [ "${SCRIPT_VARIANT}" = "msfs_install.sh" ]; then
			dpkg -i ${SCRIPT_DIR}/${DEB_AMD64_PACKAGE_BASENAME}
		elif [ "${SCRIPT_VARIANT}" = "msfs_uninstall.sh" ]; then
			dpkg -r msfs
		else
			echo "SCRIPT_VARIANT (${SCRIPT_VARIANT}) not understood"
			exit 1
		fi
	elif [ "${PKG_TYPE}" = "rpm" ]; then
		if [ "${SCRIPT_VARIANT}" = "msfs_install.sh" ]; then
			rpm -ivh ${SCRIPT_DIR}/${RPM_AMD64_PACKAGE_BASENAME}
		elif [ "${SCRIPT_VARIANT}" = "msfs_uninstall.sh" ]; then
			rpm -e msfs
		else
			echo "SCRIPT_VARIANT (${SCRIPT_VARIANT}) not understood"
			exit 1
		fi
	else
		echo "PKG_TYPE (${PKG_TYPE}) not understood"
		exit 1
	fi
elif [ "${UNAMEM}" = "aarch64" ]; then
	if [ "${PKG_TYPE}" = "deb" ]; then
		if [ "${SCRIPT_VARIANT}" = "msfs_install.sh" ]; then
			dpkg -i ${SCRIPT_DIR}/${DEB_ARM64_PACKAGE_BASENAME}
		elif [ "${SCRIPT_VARIANT}" = "msfs_uninstall.sh" ]; then
			dpkg -r msfs
		else
			echo "SCRIPT_VARIANT (${SCRIPT_VARIANT}) not understood"
			exit 1
		fi
	elif [ "${PKG_TYPE}" = "rpm" ]; then
		if [ "${SCRIPT_VARIANT}" = "msfs_install.sh" ]; then
			rpm -ivh ${SCRIPT_DIR}/${RPM_ARM64_PACKAGE_BASENAME}
		elif [ "${SCRIPT_VARIANT}" = "msfs_uninstall.sh" ]; then
			rpm -e msfs
		else
			echo "SCRIPT_VARIANT (${SCRIPT_VARIANT}) not understood"
			exit 1
		fi
	else
		echo "PKG_TYPE (${PKG_TYPE}) not understood"
		exit 1
	fi
else
	echo "uname -m (${UNAMEM}) not understood"
	exit 1
fi
