-define(EXCHANGE_POWER_EQUIPMENT_IN,<<"exchange_power_equipment_in">>).
-define(EXCHANGE_POWER_EQUIPMENT_OUT,<<"exchange_power_equipment_out">>).
-define(EXCHANGE_SOLDIER_IN,<<"exchange_soldier_in">>).

-define(EXTENSION_SPLITTER,<<"-">>).

-define(QUEUE_POWER,<<"queue_power">>).
-define(QUEUE_SOLDIER,<<"queue_soldier">>).
%%-----------------
%% hand相关的接口参数
%%-----------------
-define(EXCHANGE_POWER_DB_IN,<<"exchange_power_db_in">>).

-define(QUEUE_HAND,<<"queue_hand">>).

-define(ROUTING_KEY_HAND_IN,<<"routing_key_hand_in">>).

-define(EXCHANGE_WILLING_IN,<<"exchange_willing_in">>).
-ifndef(EXCHANGE_POWER_EQUIPMENT_OUT).
-define(EXCHANGE_POWER_EQUIPMENT_OUT,<<"exchange_power_equipment_out">>).
-endif.

-define(QUEUE_WILLING,<<"queue_willing">>).

-define(ROUTING_KEY_WILLING_IN,<<"routing_key_willing_in">>).

