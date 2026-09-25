# A collection GET's query as FACTS (field 66), a JSON fragment kube_facts.rd
# completes into a list/watch registration: the resume revision,
# timeoutSeconds, bookmarks, the field selector's name/namespace,
# sendInitialEvents, limit/continue, and the labelSelector as REQUIREMENTS —
# the structure a selector map stage evaluates (key, operator
# In|NotIn|Exists|DoesNotExist, values as a comma list), parsed once at the
# codec so no decision parses selector text:
#
#   k | !k | k=v | k==v | k!=v | k in (a,b) | k notin (a,b)
#
# Up to four requirements (a program has no loop). Every other field of
# kube_decode's frame passes through unchanged.
rest ; store 0
load 0 ; field 1 ; set 1
load 0 ; field 2 ; set 2
load 0 ; field 6 ; set 6
load 0 ; field 8 ; set 8
load 0 ; field 9 ; set 9
load 0 ; field 10 ; set 10
load 0 ; field 11 ; set 11
load 0 ; field 15 ; set 15
load 0 ; field 16 ; set 16
load 0 ; field 12 ; set 12
load 0 ; field 41 ; set 41
load 0 ; field 42 ; set 42
load 0 ; field 43 ; set 43
load 0 ; field 44 ; set 44
load 0 ; field 45 ; set 45
load 0 ; field 46 ; set 46
load 0 ; field 47 ; set 47
load 0 ; field 48 ; set 48
load 0 ; field 51 ; set 51
load 0 ; field 52 ; set 52
load 0 ; field 53 ; set 53
load 0 ; field 54 ; set 54
str "&" ; load 0 ; field 10 ; cat 2 ; after "&labelSelector=" ; before "&" ; pctdec ; store 7
str "["
# requirement 0
load 7 ; partb ',' '(' ')' 0 ; store 1
load 1 ; before " notin (" ; load 1 ; after " notin (" ; gate
load 1 ; before " in (" ; load 1 ; after " in (" ; gate ; coalesce
load 1 ; before "!=" ; load 1 ; after "!=" ; gate ; coalesce
load 1 ; before "=" ; load 1 ; after "=" ; gate ; coalesce
load 1 ; after "!" ; coalesce ; load 1 ; coalesce ; store 2
str "NotIn" ; load 1 ; after " notin (" ; gate
str "In" ; load 1 ; after " in (" ; gate ; coalesce
str "NotIn" ; load 1 ; after "!=" ; gate ; coalesce
str "In" ; load 1 ; after "=" ; gate ; coalesce
str "DoesNotExist" ; load 1 ; after "!" ; gate ; coalesce
str "Exists" ; load 1 ; gate ; coalesce ; store 3
load 1 ; after " notin (" ; before ")"
load 1 ; after " in (" ; before ")" ; coalesce
load 1 ; after "!=" ; coalesce
load 1 ; after "=" ; after "=" ; coalesce
load 1 ; after "=" ; coalesce ; store 4
str "{\"key\":\"" ; load 2 ; str "\",\"operator\":\"" ; load 3 ; str "\",\"values\":\"" ; load 4 ; str "\"}" ; cat 7 ; load 1 ; gate
# requirement 1
load 7 ; partb ',' '(' ')' 1 ; store 1
load 1 ; before " notin (" ; load 1 ; after " notin (" ; gate
load 1 ; before " in (" ; load 1 ; after " in (" ; gate ; coalesce
load 1 ; before "!=" ; load 1 ; after "!=" ; gate ; coalesce
load 1 ; before "=" ; load 1 ; after "=" ; gate ; coalesce
load 1 ; after "!" ; coalesce ; load 1 ; coalesce ; store 2
str "NotIn" ; load 1 ; after " notin (" ; gate
str "In" ; load 1 ; after " in (" ; gate ; coalesce
str "NotIn" ; load 1 ; after "!=" ; gate ; coalesce
str "In" ; load 1 ; after "=" ; gate ; coalesce
str "DoesNotExist" ; load 1 ; after "!" ; gate ; coalesce
str "Exists" ; load 1 ; gate ; coalesce ; store 3
load 1 ; after " notin (" ; before ")"
load 1 ; after " in (" ; before ")" ; coalesce
load 1 ; after "!=" ; coalesce
load 1 ; after "=" ; after "=" ; coalesce
load 1 ; after "=" ; coalesce ; store 4
str ","
str "{\"key\":\"" ; load 2 ; str "\",\"operator\":\"" ; load 3 ; str "\",\"values\":\"" ; load 4 ; str "\"}" ; cat 8 ; load 1 ; gate
# requirement 2
load 7 ; partb ',' '(' ')' 2 ; store 1
load 1 ; before " notin (" ; load 1 ; after " notin (" ; gate
load 1 ; before " in (" ; load 1 ; after " in (" ; gate ; coalesce
load 1 ; before "!=" ; load 1 ; after "!=" ; gate ; coalesce
load 1 ; before "=" ; load 1 ; after "=" ; gate ; coalesce
load 1 ; after "!" ; coalesce ; load 1 ; coalesce ; store 2
str "NotIn" ; load 1 ; after " notin (" ; gate
str "In" ; load 1 ; after " in (" ; gate ; coalesce
str "NotIn" ; load 1 ; after "!=" ; gate ; coalesce
str "In" ; load 1 ; after "=" ; gate ; coalesce
str "DoesNotExist" ; load 1 ; after "!" ; gate ; coalesce
str "Exists" ; load 1 ; gate ; coalesce ; store 3
load 1 ; after " notin (" ; before ")"
load 1 ; after " in (" ; before ")" ; coalesce
load 1 ; after "!=" ; coalesce
load 1 ; after "=" ; after "=" ; coalesce
load 1 ; after "=" ; coalesce ; store 4
str ","
str "{\"key\":\"" ; load 2 ; str "\",\"operator\":\"" ; load 3 ; str "\",\"values\":\"" ; load 4 ; str "\"}" ; cat 8 ; load 1 ; gate
# requirement 3
load 7 ; partb ',' '(' ')' 3 ; store 1
load 1 ; before " notin (" ; load 1 ; after " notin (" ; gate
load 1 ; before " in (" ; load 1 ; after " in (" ; gate ; coalesce
load 1 ; before "!=" ; load 1 ; after "!=" ; gate ; coalesce
load 1 ; before "=" ; load 1 ; after "=" ; gate ; coalesce
load 1 ; after "!" ; coalesce ; load 1 ; coalesce ; store 2
str "NotIn" ; load 1 ; after " notin (" ; gate
str "In" ; load 1 ; after " in (" ; gate ; coalesce
str "NotIn" ; load 1 ; after "!=" ; gate ; coalesce
str "In" ; load 1 ; after "=" ; gate ; coalesce
str "DoesNotExist" ; load 1 ; after "!" ; gate ; coalesce
str "Exists" ; load 1 ; gate ; coalesce ; store 3
load 1 ; after " notin (" ; before ")"
load 1 ; after " in (" ; before ")" ; coalesce
load 1 ; after "!=" ; coalesce
load 1 ; after "=" ; after "=" ; coalesce
load 1 ; after "=" ; coalesce ; store 4
str ","
str "{\"key\":\"" ; load 2 ; str "\",\"operator\":\"" ; load 3 ; str "\",\"values\":\"" ; load 4 ; str "\"}" ; cat 8 ; load 1 ; gate
str "]" ; cat 6 ; store 6
# the query, each parameter found as "&name="
str "&" ; load 0 ; field 10 ; cat 2 ; store 5
str ",\"rv\":\"" ;
load 5 ; after "&resourceVersion=" ; before "&" ;
str "\",\"ts\":\"" ;
load 5 ; after "&timeoutSeconds=" ; before "&" ;
str "\",\"bm\":\"" ;
str "{\"=true\":\"1\"}" ; str "=" ; load 5 ; after "&allowWatchBookmarks=" ; before "&" ; cat 2 ; jsonat ;
str "\",\"fsn\":\"" ;
str "," ; load 5 ; after "&fieldSelector=" ; before "&" ; pctdec ; cat 2 ; after ",metadata.name=" ; before "," ; store 1 ; load 1 ; after "=" ; load 1 ; coalesce ;
str "\",\"fsns\":\"" ;
str "," ; load 5 ; after "&fieldSelector=" ; before "&" ; pctdec ; cat 2 ; after ",metadata.namespace=" ; before "," ; store 1 ; load 1 ; after "=" ; load 1 ; coalesce ;
str "\",\"sie\":\"" ;
load 5 ; after "&sendInitialEvents=" ; before "&" ;
str "\",\"lim\":\"" ;
load 5 ; after "&limit=" ; before "&" ;
str "\",\"cont\":\"" ;
load 5 ; after "&continue=" ; before "&" ; pctdec ;
str "\",\"sel\":" ;
load 6 ; cat 18 ; set 66
finish
