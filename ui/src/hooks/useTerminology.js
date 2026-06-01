import { AWSTerminology } from '../theme/aws/terminology';
import { azureTerminology } from '../theme/azure/terminology';
import { gcpTerminology } from '../theme/gcp/terminology';
const terminologyByCloud = {
    aws: AWSTerminology,
    azure: azureTerminology,
    gcp: gcpTerminology,
};
/** Returns the terminology map for the given cloud, falling back to AWS. */
export function useActiveTerminology(cloud) {
    return terminologyByCloud[cloud] ?? AWSTerminology;
}
