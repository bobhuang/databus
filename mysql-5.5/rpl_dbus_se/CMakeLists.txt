# Copyright (c) 2006, 2010, Oracle and/or its affiliates. All rights reserved.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA

SET(RPL_DBUS_PLUGIN_STATIC  "rpl_dbus")
SET(RPL_DBUS_PLUGIN_MANDATORY TRUE)

SET(RPL_DBUS_SOURCES  ha_rpl_dbus.cc ha_rpl_dbus.h tcp_socket.h tcp_socket.cc dbus_config.h
                       dbus_config.cc dbus_client.h dbus_client.h dbus_client.cc
                       dbus_state.h dbus_state.cc dbus_tcp_client.h dbus_tcp_client.cc dbus_slave_angel.h 
                       dbus_slave_angel.cc rpl_dbus_crc.h rpl_dbus_crc.cc)
MYSQL_ADD_PLUGIN(rpl_dbus ${RPL_DBUS_SOURCES} STORAGE_ENGINE MANDATORY RECOMPILE_FOR_EMBEDDED)
