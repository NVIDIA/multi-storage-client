# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared virtual-manifest runtime limits."""

DEFAULT_ROW_GROUP_CACHE_SIZE_BYTES = 64 * 1024 * 1024
MAX_ROW_GROUP_CACHE_SIZE_BYTES = (1 << 63) - 1
