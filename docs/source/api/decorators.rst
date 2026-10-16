Decorators
==========

Decorators are able to register callback functions for handling updates in a much easier and cleaner way compared to
:doc:`Handlers <handlers>`; they do so by instantiating the correct handler and calling
:meth:`~pyrogram.Client.add_handler` automatically. All you need to do is adding the decorators on top of your
functions.

.. code-block:: python

    from pyrogram import Client

    app = Client("my_account")


    @app.on_message()
    def log(client, message):
        print(message)


    app.run()

.. contents:: Contents
    :backlinks: none
    :depth: 1
    :local:

-----

.. currentmodule:: pyrogram

Index
-----

.. hlist::
    :columns: 3

    - :meth:`~Client.on_bot_business_connect`
    - :meth:`~Client.on_message`
    - :meth:`~Client.on_bot_business_message`
    - :meth:`~Client.on_edited_message`
    - :meth:`~Client.on_edited_bot_business_message`
    - :meth:`~Client.on_callback_query`
    - :meth:`~Client.on_shipping_query`
    - :meth:`~Client.on_pre_checkout_query`
    - :meth:`~Client.on_message_reaction_updated`
    - :meth:`~Client.on_message_reaction_count_updated`
    - :meth:`~Client.on_inline_query`
    - :meth:`~Client.on_chosen_inline_result`
    - :meth:`~Client.on_chat_member_updated`
    - :meth:`~Client.on_chat_join_request`
    - :meth:`~Client.on_deleted_messages`
    - :meth:`~Client.on_deleted_bot_business_message`
    - :meth:`~Client.on_user_status`
    - :meth:`~Client.on_story`
    - :meth:`~Client.on_poll`
    - :meth:`~Client.on_disconnect`
    - :meth:`~Client.on_raw_update`
    - :meth:`~Client.on_error`
    - :meth:`~Client.on_chat_join_result`
    - :meth:`~Client.on_guest_query`
    - :meth:`~Client.on_managed_bot_updated`
    - :meth:`~Client.on_business_connected_bot`
    - :meth:`~Client.on_pending_message`
    - :meth:`~Client.on_message_draft_stopped`
    - :meth:`~Client.on_ton_wallet_state`
    - :meth:`~Client.on_ton_wallet_gasless_transfers_info`
    - :meth:`~Client.on_ton_connect_session`
    - :meth:`~Client.on_ton_connect_pending_disconnect`
    - :meth:`~Client.on_ephemeral_message`
    - :meth:`~Client.on_edited_ephemeral_message`
    - :meth:`~Client.on_deleted_ephemeral_messages`

-----

Details
-------

.. Decorators
.. autodecorator:: pyrogram.Client.on_bot_business_connect()
.. autodecorator:: pyrogram.Client.on_message()
.. autodecorator:: pyrogram.Client.on_bot_business_message()
.. autodecorator:: pyrogram.Client.on_edited_message()
.. autodecorator:: pyrogram.Client.on_edited_bot_business_message()
.. autodecorator:: pyrogram.Client.on_callback_query()
.. autodecorator:: pyrogram.Client.on_shipping_query()
.. autodecorator:: pyrogram.Client.on_pre_checkout_query()
.. autodecorator:: pyrogram.Client.on_message_reaction_updated()
.. autodecorator:: pyrogram.Client.on_message_reaction_count_updated()
.. autodecorator:: pyrogram.Client.on_inline_query()
.. autodecorator:: pyrogram.Client.on_chosen_inline_result()
.. autodecorator:: pyrogram.Client.on_chat_member_updated()
.. autodecorator:: pyrogram.Client.on_chat_join_request()
.. autodecorator:: pyrogram.Client.on_deleted_messages()
.. autodecorator:: pyrogram.Client.on_deleted_bot_business_message()
.. autodecorator:: pyrogram.Client.on_user_status()
.. autodecorator:: pyrogram.Client.on_story()
.. autodecorator:: pyrogram.Client.on_poll()
.. autodecorator:: pyrogram.Client.on_disconnect()
.. autodecorator:: pyrogram.Client.on_raw_update()
.. autodecorator:: pyrogram.Client.on_error()
.. autodecorator:: pyrogram.Client.on_chat_join_result()
.. autodecorator:: pyrogram.Client.on_guest_query()
.. autodecorator:: pyrogram.Client.on_managed_bot_updated()
.. autodecorator:: pyrogram.Client.on_business_connected_bot()
.. autodecorator:: pyrogram.Client.on_pending_message()
.. autodecorator:: pyrogram.Client.on_message_draft_stopped()
.. autodecorator:: pyrogram.Client.on_ton_wallet_state()
.. autodecorator:: pyrogram.Client.on_ton_wallet_gasless_transfers_info()
.. autodecorator:: pyrogram.Client.on_ton_connect_session()
.. autodecorator:: pyrogram.Client.on_ton_connect_pending_disconnect()
.. autodecorator:: pyrogram.Client.on_ephemeral_message()
.. autodecorator:: pyrogram.Client.on_edited_ephemeral_message()
.. autodecorator:: pyrogram.Client.on_deleted_ephemeral_messages()
