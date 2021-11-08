import sqlalchemy
from sqlalchemy import create_engine, insert, select
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from models import Base, Order

# interact with the database
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

'''
You may assume that ‘buy_currency’ and ‘sell_currency’ will be either ‘Algorand’ or 'Ethereum’.
In future assignments, “sender_pk” will be the public-key of the sender 
(on the platform specified by sell_currency), 
and “receiver_pk” will be the key (also controlled by the order’s originator) 
where tokens on the platform “buy_currency” will be sent if the order is filled. 
In our example, the individual who made this order is trying to sell 2342.31 Eth 
(using its ‘AAAAC3Nza…’ key) in exchange for 1245.00 Algorand (receiving on its ‘0xd1B77a92…’ key).
'''

'''
The order book should accept orders and store them in database. 
When a new order comes in, the system should try to “fill” the order by matching 
it with an existing order (or orders) in the database. 
'''


def process_order(order):
    # Your code here
    # 1. Insert the order into the database
    add_order = Order(sender_pk=order['sender_pk'], receiver_pk=order['receiver_pk'],
                      buy_currency=order['buy_currency'], sell_currency=order['sell_currency'],
                      buy_amount=order['buy_amount'], sell_amount=order['sell_amount'])
    session.add(add_order)
    session.commit()

    # new_order = session.insert().values(buy_currency=order['buy_currency'],
    #                                     sell_currency=order['sell_currency'],
    #                                     buy_amount=order['buy_amount'],
    #                                     sell_amount=order['sell_amount'],
    #                                     sender_pk=order['sender_pk'],
    #                                     receiver_pk=order['receiver_pk'])
    # engine.connect().execute(new_order)

    # 2. Check if there are any existing orders that match.
    # check = select([session]).where((session.c.filled is None) &
    #                                 (session.c.buy_currency == order['sell_currency']) &
    #                                 session.c.sell_currency == order['buy_currency'] &
    #                                 session.c.sell_amount / session.c.buy_amount >= order['buy_amount'] / order[
    #                                     'sell_amount'])
    # engine.connect().execute(check)

    existing_order = session.query(Order).filter(Order.filled is None,
                                                 Order.buy_currency == order['sell_currency'],
                                                 Order.sell_currency == order['buy_currency'],
                                                 Order.sell_amount / Order.buy_amount >= order['buy_amount'] / order[
                                                     'sell_amount']).first()

    # Each order should match at most one other
    # one = session.query(check).first()
    # if not one:
    #     print('No result found.')
    #
    # else:

    # 3. If a match is found between order and existing_order:

    # update1 = session.update().where(session.c.id == check.c.id).values(timestamp=datetime.now())
    # engine.connect().execute(update1)
    # update2 = session.update().where(and_(session.c.buy_currency=order['buy_currency'],
    #                                 session.c.sell_currency=order['sell_currency'],
    #                                 session.c.buy_amount=order['buy_amount'],
    #                                 session.c.sell_amount=order['sell_amount'],
    #                                 session.c.sender_pk=order['sender_pk'],
    #                                 session.c.receiver_pk=order['receiver_pk'])).values(timestamp=datetime.now())
    # engine.connect().execute(update2)

    if existing_order:
        new_order = session.query(Order).filter(Order.sender_pk == order['sender_pk'],
                                                Order.receiver_pk == order['receiver_pk'],
                                                Order.buy_currency == order['buy_currency'],
                                                Order.sell_currency == order['sell_currency'],
                                                Order.buy_amount == order['buy_amount'],
                                                Order.sell_amount == order['sell_amount']).first()

        # Set the filled field to be the current timestamp on both orders
        new_order.filled = datetime.now()
        existing_order.filled = datetime.now()

        # Set counterparty_id to be the id of the other order
        new_order.counterparty_id = existing_order.id
        existing_order.counterparty_id = new_order.id

        # If one of the orders is not completely filled
        # (i.e. the counterparty’s sell_amount is less than buy_amount):
        if existing_order.buy_amount > new_order.sell_amount:
            # Existing order is unfilled. Create a new order for remaining balance
            child_order = Order(buy_amount=(existing_order.buy_amount-new_order.sell_amount),
                                creater_id=existing_order.id,
                                sender_pk=existing_order.receiver_pk,
                                receiver_pk=existing_order.sender_pk,
                                buy_currency=existing_order.buy_currency,
                                sell_currency=existing_order.sell_currency,
                                sell_amount=existing_order.buy_amount/existing_order.sell_amount*(existing_order.buy_amount-new_order.sell_amount))
            session.add(child_order)
            # You can then try to fill the new order.....


        if new_order.buy_amount > existing_order.sell_amount:
            # New order is unfilled. Create a new order for remaining balance
            child_order = Order(buy_amount=(new_order.buy_amount - existing_order.sell_amount),
                                creater_id=new_order.id,
                                sender_pk=new_order.receiver_pk,
                                receiver_pk=new_order.sender_pk,
                                buy_currency=new_order.buy_currency,
                                sell_currency=new_order.sell_currency,
                                sell_amount=new_order.buy_amount / new_order.sell_amount * (
                                            new_order.buy_amount - existing_order.sell_amount))
            session.add(child_order)

        session.commit()
    session.commit()






