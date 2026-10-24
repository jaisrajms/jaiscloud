import {
  getSubscriptionIam,
  getTopicIam,
  putSubscriptionIam,
  putTopicIam,
} from '../../api/gcp/pubsub'
import { IamPolicyPanel } from '../common/IamPolicyPanel'

/** Editable IAM policy for a Pub/Sub topic or subscription. */
export function IamPanel({
  kind,
  name,
}: {
  kind: 'topic' | 'subscription'
  name: string
}) {
  const load = kind === 'topic' ? getTopicIam : getSubscriptionIam
  const savePolicy = kind === 'topic' ? putTopicIam : putSubscriptionIam
  return (
    <IamPolicyPanel
      title={`${kind === 'topic' ? 'Topic' : 'Subscription'} IAM policy`}
      queryKey={['gcp', 'pubsub', kind, name, 'iam']}
      load={() => load(name)}
      save={(policy) => savePolicy(name, policy)}
      defaultRole={kind === 'topic' ? 'roles/pubsub.publisher' : 'roles/pubsub.subscriber'}
    />
  )
}
