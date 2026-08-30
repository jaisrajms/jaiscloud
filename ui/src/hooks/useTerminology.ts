import { AWSTerminology } from '../theme/aws/terminology'
import { azureTerminology } from '../theme/azure/terminology'
import { gcpTerminology } from '../theme/gcp/terminology'

type TerminologyMap = Record<string, string>

const terminologyByCloud: Record<string, TerminologyMap> = {
  aws: AWSTerminology,
  azure: azureTerminology as TerminologyMap,
  gcp: gcpTerminology as TerminologyMap,
}

/** Returns the terminology map for the given cloud, falling back to AWS. */
export function useActiveTerminology(cloud: string): TerminologyMap {
  return terminologyByCloud[cloud] ?? AWSTerminology
}
