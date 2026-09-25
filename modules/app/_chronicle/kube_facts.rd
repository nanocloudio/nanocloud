# The facts a GET needs past the router, in the one slot a GET leaves free —
# its body (6). A LIST, a WATCH and a GET of one object are all served by the
# watch chain out of the store, so all are REGISTRATIONS: kind, apiVersion;
# for a collection, list or watch, the List envelope's head and the query's
# facts (kube_sel.rd, 66); for a named object, "g":"1". Any other request
# keeps its body.
#
# The resource's Kind is a lookup in a literal table (data, like discovery);
# an unknown resource has kind "", and its List is the generic `List`. "if" is
# GATE/COALESCE over one-key table lookups — the program has no branch. Every
# key and every looked-up value carries a leading "=", so an EMPTY value never
# names the table itself (an empty path is the whole document).
rest ; store 0
load 0 ; field 1 ; set 1
load 0 ; field 2 ; set 2
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
# /api or /apis; is the path namespaced
str "{\"=api\":\"1\"}" ; str "=" ; load 0 ; field 41 ; cat 2 ; jsonat ; store 2
str "{\"=apis\":\"1\"}" ; str "=" ; load 0 ; field 41 ; cat 2 ; jsonat ; store 7
str "{\"=namespaces\":\"1\"}" ; str "=" ; load 0 ; field 43 ; cat 2 ; jsonat ; store 1
str "{\"=namespaces\":\"1\"}" ; str "=" ; load 0 ; field 44 ; cat 2 ; jsonat ; store 5
# the resource: /api/v1/[namespaces/<ns>/]<res>, /apis/<g>/<v>/[namespaces/<ns>/]<res>
load 0 ; field 45 ; load 1 ; gate ; load 0 ; field 43 ; coalesce ; load 2 ; gate
load 0 ; field 46 ; load 5 ; gate ; load 0 ; field 44 ; coalesce ; load 7 ; gate ; coalesce ; store 3
# its name, "="-prefixed so a namespaced COLLECTION (empty name) is "=" and
# never falls through to the namespace segment; "=" alone is a collection
str "=" ; load 0 ; field 46 ; cat 2 ; load 0 ; field 45 ; load 1 ; gate ; gate ; str "=" ; load 0 ; field 44 ; cat 2 ; coalesce ; load 2 ; gate
str "=" ; load 0 ; field 47 ; cat 2 ; load 0 ; field 46 ; load 5 ; gate ; gate ; str "=" ; load 0 ; field 45 ; cat 2 ; coalesce ; load 7 ; gate ; coalesce ; store 4
# apiVersion: v1, or <group>/<version>
load 0 ; field 42 ; load 2 ; gate
load 0 ; field 42 ; str "/" ; load 0 ; field 43 ; cat 3 ; load 7 ; gate ; coalesce ; store 5
str "{\"=pods\":\"Pod\",\"=services\":\"Service\",\"=configmaps\":\"ConfigMap\",\"=secrets\":\"Secret\",\"=namespaces\":\"Namespace\",\"=nodes\":\"Node\",\"=events\":\"Event\",\"=endpoints\":\"Endpoints\",\"=serviceaccounts\":\"ServiceAccount\",\"=persistentvolumeclaims\":\"PersistentVolumeClaim\",\"=persistentvolumes\":\"PersistentVolume\",\"=resourcequotas\":\"ResourceQuota\",\"=limitranges\":\"LimitRange\",\"=deployments\":\"Deployment\",\"=replicasets\":\"ReplicaSet\",\"=statefulsets\":\"StatefulSet\",\"=daemonsets\":\"DaemonSet\",\"=jobs\":\"Job\",\"=cronjobs\":\"CronJob\",\"=horizontalpodautoscalers\":\"HorizontalPodAutoscaler\",\"=networkpolicies\":\"NetworkPolicy\",\"=ingresses\":\"Ingress\",\"=roles\":\"Role\",\"=rolebindings\":\"RoleBinding\",\"=clusterroles\":\"ClusterRole\",\"=clusterrolebindings\":\"ClusterRoleBinding\",\"=leases\":\"Lease\",\"=endpointslices\":\"EndpointSlice\",\"=storageclasses\":\"StorageClass\",\"=poddisruptionbudgets\":\"PodDisruptionBudget\",\"=customresourcedefinitions\":\"CustomResourceDefinition\",\"=priorityclasses\":\"PriorityClass\"}" ; str "=" ; load 3 ; cat 2 ; jsonat ; store 6
# a collection (list or watch): the registration's facts —
#   {"k","av","l": "1" for a LIST, "h": the List envelope's head (escaped),
#    …the query's facts (kube_sel.rd, 66)}
str "{\"k\":\"" ;
load 6 ;
str "\",\"av\":\"" ;
load 5 ;
str "\",\"l\":\"" ;
str "{\"=\":\"1\"}" ; str "=" ; str "{\"=true\":\"1\",\"=1\":\"1\"}" ; str "=" ; str "&" ; load 0 ; field 10 ; cat 2 ; after "&watch=" ; before "&" ; cat 2 ; jsonat ; cat 2 ; jsonat ;
str "\",\"h\":\"{\\\"kind\\\":\\\"" ;
load 6 ;
str "List\\\",\\\"apiVersion\\\":\\\"" ;
load 5 ;
str "\\\",\\\"items\\\":[\"" ;
load 0 ; field 66 ;
str ",\"tb\":\"" ; load 0 ; field 15 ; str "" ; coalesce ; str "\"" ;
str "}" ; cat 16
str "{\"=\":\"1\"}" ; load 4 ; jsonat ; gate
# a named object: a GET registration — {"k","av","g":"1","tb","tl","lb"}; a
# log read's `tailLines` and `limitBytes` ride along (empty when not asked)
str "{\"k\":\"" ; load 6 ; str "\",\"av\":\"" ; load 5 ; str "\",\"g\":\"1\",\"tb\":\"" ; load 0 ; field 15 ; str "" ; coalesce ;
str "\",\"tl\":\"" ; str "&" ; load 0 ; field 10 ; cat 2 ; after "&tailLines=" ; before "&" ;
str "\",\"lb\":\"" ; str "&" ; load 0 ; field 10 ; cat 2 ; after "&limitBytes=" ; before "&" ;
str "\"}" ; cat 11 ; coalesce
# only for a GET (method 1); any other request keeps its own body
str "{\"=1\":\"1\"}" ; str "=" ; load 0 ; field 2 ; cat 2 ; jsonat ; gate
load 0 ; field 6 ; coalesce ; set 6
finish
