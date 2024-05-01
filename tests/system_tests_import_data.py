#!/usr/bin/env python3

'''
This code is licensed under the GNU Affero General Public License (AGPL),
version 3, or (at your option) any later version.
'''

import unittest
from unittest import mock
from unittest.mock import call
from importer import cbioportalImporter

common_part = ('-Dspring.profiles.active=dbcp', '-cp', 'test.jar')

class DataImporterTests(unittest.TestCase):
    '''
    Tests of commands produced by scripts
    '''

    def setUp(self):
        self.maxDiff = None

    @mock.patch('importer.cbioportalImporter.locate_jar')
    @mock.patch('importer.cbioportalImporter.run_java')
    def test_full_study_load(self, run_java, locate_jar):
        '''
        Tests java commands full study load produces
        '''
        locate_jar.return_value = "test.jar"

        study_directory = 'test_data/study_es_0'
        args = ['--study_directory', study_directory]
        parsed_args = cbioportalImporter.interface(args)
        cbioportalImporter.main(parsed_args)

        self.assertListEqual(run_java.call_args_list, [
            call(*common_part, 'org.mskcc.cbio.portal.util.VersionUtil',),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportTypesOfCancers', f'{study_directory}/data_cancer_type.txt', 'false', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.RemoveCancerStudy', 'study_es_0', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportCancerStudy', f'{study_directory}/meta_study.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportClinicalData', '--meta', f'{study_directory}/meta_clinical_samples.txt', '--loadMode', 'bulkload', '--data', f'{study_directory}/data_clinical_samples.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportResourceDefinition', '--meta', f'{study_directory}/meta_resource_definition.txt', '--loadMode', 'bulkload', '--data', f'{study_directory}/data_resource_definition.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportResourceData', '--meta', f'{study_directory}/meta_resource_sample.txt', '--loadMode', 'bulkload', '--data', f'{study_directory}/data_resource_sample.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportClinicalData', '--meta', f'{study_directory}/meta_clinical_patients.txt', '--loadMode', 'bulkload', '--data', f'{study_directory}/data_clinical_patients.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportCopyNumberSegmentData', '--meta', f'{study_directory}/meta_cna_hg19_seg.txt', '--loadMode', 'bulkload', '--data', f'{study_directory}/data_cna_hg19.seg', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_cna_log2.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_cna_log2.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_expression_median.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_expression_median.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_generic_assay_patient_test.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_generic_assay_patient_test.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportGisticData', '--data', f'{study_directory}/data_gistic_genes_amp.txt', '--study', 'study_es_0', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_methylation_hm27.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_methylation_hm27.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_mutational_signature.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_mutational_signature.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_mutations_extended.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_mutations_extended.maf', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportResourceData', '--meta', f'{study_directory}/meta_resource_patient.txt', '--loadMode', 'bulkload', '--data', f'{study_directory}/data_resource_patient.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportResourceData', '--meta', f'{study_directory}/meta_resource_study.txt', '--loadMode', 'bulkload', '--data', f'{study_directory}/data_resource_study.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_treatment_ec50.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_treatment_ec50.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_treatment_ic50.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_treatment_ic50.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_structural_variants.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_structural_variants.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_cna_discrete.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_cna_discrete.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_expression_median_Zscores.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_expression_median_Zscores.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_gsva_scores.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_gsva_scores.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_gsva_pvalues.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_gsva_pvalues.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportGenePanelProfileMap', '--meta', f'{study_directory}/meta_gene_panel_matrix.txt', '--data', f'{study_directory}/data_gene_panel_matrix.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportSampleList', f'{study_directory}/case_lists/cases_cna.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportSampleList', f'{study_directory}/case_lists/cases_cnaseq.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportSampleList', f'{study_directory}/case_lists/cases_custom.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportSampleList', f'{study_directory}/case_lists/cases_sequenced.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportSampleList', f'{study_directory}/case_lists/cases_test.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.AddCaseList', 'study_es_0', 'all', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.UpdateCancerStudy', 'study_es_0', 'AVAILABLE', '--noprogress')])

    @mock.patch('importer.cbioportalImporter.locate_jar')
    @mock.patch('importer.cbioportalImporter.run_java')
    def test_incremental_load(self, run_java, locate_jar):
        '''
        Tests java commands incremental load produces
        '''
        locate_jar.return_value = "test.jar"

        data_directory = 'test_data/study_es_0_inc'
        args = ['--data_directory', data_directory]
        parsed_args = cbioportalImporter.interface(args)
        cbioportalImporter.main(parsed_args)

        self.assertListEqual(run_java.call_args_list, [
            call(*common_part, 'org.mskcc.cbio.portal.util.VersionUtil',),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportClinicalData', '--overwrite-existing', '--meta', f'{data_directory}/meta_clinical_patients.txt', '--loadMode', 'bulkload', '--data', f'{data_directory}/data_clinical_patients.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportClinicalData', '--overwrite-existing', '--meta', f'{data_directory}/meta_clinical_samples.txt', '--loadMode', 'bulkload', '--data', f'{data_directory}/data_clinical_samples.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--overwrite-existing', '--meta', f'{data_directory}/meta_mutations_extended.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{data_directory}/data_mutations_extended.maf', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.UpdateCaseListsSampleIds', '--meta', f'{data_directory}/meta_clinical_samples.txt', '--case-lists', f'{data_directory}/case_lists')])


if __name__ == '__main__':
    unittest.main(buffer=True)
