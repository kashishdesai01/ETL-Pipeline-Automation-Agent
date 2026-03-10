-- This function calculates a discounted price.
CREATE OR REPLACE FUNCTION calculate_discount(price NUMBER, discount_percent NUMBER) 
RETURN NUMBER IS
   discount_amount NUMBER;
BEGIN
   discount_amount := price * (discount_percent / 100);
   RETURN price - discount_amount;
END;
/
