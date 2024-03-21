/*
 * Copyright (c) 2015 Memorial Sloan-Kettering Cancer Center.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY, WITHOUT EVEN THE IMPLIED WARRANTY OF MERCHANTABILITY OR FITNESS
 * FOR A PARTICULAR PURPOSE. The software and documentation provided hereunder
 * is on an "as is" basis, and Memorial Sloan-Kettering Cancer Center has no
 * obligations to provide maintenance, support, updates, enhancements or
 * modifications. In no event shall Memorial Sloan-Kettering Cancer Center be
 * liable to any party for direct, indirect, special, incidental or
 * consequential damages, including lost profits, arising out of the use of this
 * software and its documentation, even if Memorial Sloan-Kettering Cancer
 * Center has been advised of the possibility of such damage.
 */

/*
 * This file is part of cBioPortal.
 *
 * cBioPortal is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package org.mskcc.cbio.portal.scripts.incremental;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.*;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.ClinicalData;
import org.mskcc.cbio.portal.model.Patient;
import org.mskcc.cbio.portal.model.Sample;
import org.mskcc.cbio.portal.scripts.ImportClinicalData;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Tests Incremental Import of Sample Clinical Data.
 *
 * @author Ruslan Forostianov
 * @author Pieter Lukasse
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
@Rollback
@Transactional
public class ITIncrementalSamplesImport {

    public static final String STUDY_ID = "study_tcga_pub";
    private CancerStudy cancerStudy;
    private final String UPDATE_TCGA_SAMPLE_ID  = "TCGA-A1-A0SD-01";

    @Before
    public void setUp() throws DaoException {
        cancerStudy = DaoCancerStudy.getCancerStudyByStableId(STUDY_ID);
    }
    /**
     * Test inserting new sample for existing patient
     */
	@Test
    public void testInsertNewSampleForExistingPatient() throws DaoException {
        /**
         * prepare a new patient without samples
         */
        Patient patient = new Patient(cancerStudy, "TEST-INC-TCGA-P1");
        int internalPatientId = DaoPatient.addPatient(patient);

        String newSampleId = "TEST-INC-TCGA-P1-S1";
        File singleTcgaSampleFolder = new File("src/test/resources/incremental/insert_single_tcga_sample/");
        File metaFile = new File(singleTcgaSampleFolder, "meta_clinical_sample.txt");
        File dataFile = new File(singleTcgaSampleFolder, "clinical_data_single_SAMPLE.txt");

        ImportClinicalData importClinicalData = new ImportClinicalData(new String[] {
                "--meta", metaFile.getAbsolutePath(),
                "--data", dataFile.getAbsolutePath(),
                "--overwrite-existing",
        });
        importClinicalData.run();

        List<Sample> samples = DaoSample.getSamplesByPatientId(internalPatientId);
        assertEquals("A new sample has to be attached to the patient", 1, samples.size());
        Sample sample = samples.get(0);
        assertEquals(newSampleId, sample.getStableId());

        List<ClinicalData> clinicalData = DaoClinicalData.getSampleData(cancerStudy.getInternalId(), List.of(newSampleId));
        Map<String, String> sampleAttrs = clinicalData.stream().collect(Collectors.toMap(ClinicalData::getAttrId, ClinicalData::getAttrVal));
        assertEquals(Map.of(
                "SUBTYPE", "basal-like",
                "OS_STATUS", "1:DECEASED",
                "OS_MONTHS", "12.34",
                "DFS_STATUS", "1:Recurred/Progressed"), sampleAttrs);
	}

    /**
     * Test inserting new sample for nonexistent patient.
     * EXPECTED RESULTS:
     * 1. The new patient entry has to be inserted
     * 2. Sample and all its clinical attributes have to be inserted
     */
    @Test
    public void testInsertNewSampleForNonexistentPatient() throws DaoException {
        String newPatientId = "TEST-INC-TCGA-P2";
        String newSampleId = "TEST-INC-TCGA-P2-S1";
        File singleTcgaSampleFolder = new File("src/test/resources/incremental/insert_single_tcga_sample_for_nonexistent_patient/");
        File metaFile = new File(singleTcgaSampleFolder, "meta_clinical_sample.txt");
        File dataFile = new File(singleTcgaSampleFolder, "clinical_data_single_SAMPLE.txt");

        ImportClinicalData importClinicalData = new ImportClinicalData(new String[] {
                "--meta", metaFile.getAbsolutePath(),
                "--data", dataFile.getAbsolutePath(),
                "--overwrite-existing",
        });
        importClinicalData.run();

        Patient newPatient = DaoPatient.getPatientByCancerStudyAndPatientId(cancerStudy.getInternalId(), newPatientId);
        assertNotNull("The new patient has to be created.", newPatient);

        List<Sample> samples = DaoSample.getSamplesByPatientId(newPatient.getInternalId());
        assertEquals("A new sample has to be attached to the patient", 1, samples.size());
        Sample sample = samples.get(0);
        assertEquals(newSampleId, sample.getStableId());

        List<ClinicalData> clinicalData = DaoClinicalData.getSampleData(cancerStudy.getInternalId(), List.of(newSampleId));
        Map<String, String> sampleAttrs = clinicalData.stream().collect(Collectors.toMap(ClinicalData::getAttrId, ClinicalData::getAttrVal));
        assertEquals(Map.of(
                "SUBTYPE", "Luminal A",
                "OS_STATUS", "0:LIVING",
                "OS_MONTHS", "23.45",
                "DFS_STATUS", "1:Recurred/Progressed",
                "DFS_MONTHS", "100"), sampleAttrs);
    }

    /**
     * Test reloading sample clinical attributes
     */
    @Test
    public void testReloadSampleClinicalAttributes() throws DaoException {
        /**
         * Add to a tcga sample some clinical attributes (test data sets doesn't have any)
         */
        Sample tcgaSample = DaoSample.getSampleByCancerStudyAndSampleId(cancerStudy.getInternalId(),
                UPDATE_TCGA_SAMPLE_ID);
        DaoClinicalData.addSampleDatum(tcgaSample.getInternalId(), "SUBTYPE", "Luminal A");
        DaoClinicalData.addSampleDatum(tcgaSample.getInternalId(), "OS_STATUS", "0:LIVING");
        DaoClinicalData.addSampleDatum(tcgaSample.getInternalId(), "OS_MONTHS", "34.56");

        File singleTcgaSampleFolder = new File("src/test/resources/incremental/update_single_tcga_sample/");
        File metaFile = new File(singleTcgaSampleFolder, "meta_clinical_sample.txt");
        File dataFile = new File(singleTcgaSampleFolder, "clinical_data_single_SAMPLE.txt");

        ImportClinicalData importClinicalData = new ImportClinicalData(new String[] {
                "--meta", metaFile.getAbsolutePath(),
                "--data", dataFile.getAbsolutePath(),
                "--overwrite-existing",
        });
        importClinicalData.run();

        List<ClinicalData> clinicalData = DaoClinicalData.getSampleData(cancerStudy.getInternalId(), List.of(UPDATE_TCGA_SAMPLE_ID));
        Map<String, String> sampleAttrs = clinicalData.stream().collect(Collectors.toMap(ClinicalData::getAttrId, ClinicalData::getAttrVal));
        assertEquals(Map.of(
                "OS_STATUS", "1:DECEASED",
                "OS_MONTHS", "45.67",
                "DFS_STATUS", "1:Recurred/Progressed",
                "DFS_MONTHS", "123"), sampleAttrs);
    }
}
