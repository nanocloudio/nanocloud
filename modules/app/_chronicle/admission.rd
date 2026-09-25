# Admission's decode: the record ad_cnt answers (admission.uproc Cnt, as a
# frame), with the policy APPLIED — upstream's order, mutate then validate:
#
#   defaults   each `path=raw` of the policy's list inserted where the object
#              has nothing (JSONDEF: top level or one object down)
#   required   each path of the policy's list looked up in the DEFAULTED
#              object (JSONAT): 70..73 the paths, 74..77 their values, ABSENT
#              where the object lacks them — which `ad` reads with has()
#
# The policy's lists are data, so their paths are too: a program has no loop,
# and reads them at fixed positions (three defaults, four required fields).
rest ; store 0
load 0 ; field 45 ; set 45        # the quota count
load 0 ; field 42 ; set 42
load 0 ; field 50 ; set 50
load 0 ; field 60 ; set 60        # verb
load 0 ; field 64 ; set 64        # the policy read's status
load 0 ; field 30 ; set 30
load 0 ; field 31 ; set 31
load 0 ; field 32 ; set 32
load 0 ; field 33 ; set 33
load 0 ; field 34 ; set 34
load 0 ; field 36 ; set 36
load 0 ; field 37 ; set 37
load 0 ; field 41 ; store 1       # defaults=<path>=<raw>,…
load 0 ; field 63                 # the object
load 1 ; part ',' 0 ; before "=" ; load 1 ; part ',' 0 ; after "=" ; jsondef
load 1 ; part ',' 1 ; before "=" ; load 1 ; part ',' 1 ; after "=" ; jsondef
load 1 ; part ',' 2 ; before "=" ; load 1 ; part ',' 2 ; after "=" ; jsondef
store 2 ; load 2 ; set 63
load 0 ; field 40 ; store 3       # required=<path>,…
load 3 ; part ',' 0 ; set 70 ; load 2 ; load 3 ; part ',' 0 ; jsonat ; set 74
load 3 ; part ',' 1 ; set 71 ; load 2 ; load 3 ; part ',' 1 ; jsonat ; set 75
load 3 ; part ',' 2 ; set 72 ; load 2 ; load 3 ; part ',' 2 ; jsonat ; set 76
load 3 ; part ',' 3 ; set 73 ; load 2 ; load 3 ; part ',' 3 ; jsonat ; set 77
finish
